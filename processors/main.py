"""Central orchestration of all processing steps."""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List

import pandas as pd

try:  # pragma: no cover - runtime convenience
    from . import (
        aggregation,
        bikes,
        chantiers,
        comptage_velo,
        config,
        metrics,
        qualite_service,
        referentiel_troncons,
        reports,
        storage,
        traffic,
        validations,
        weather,
    )
    from .base import PipelineResult, QualityReport
    from .config import get_config
    from .storage import InputReader, OutputWriter
except ImportError:  # pragma: no cover - executed when run as script directly
    sys.path.append(str(Path(__file__).resolve().parent.parent))
    from processors import aggregation
    from processors import bikes
    from processors import chantiers
    from processors import comptage_velo
    from processors import config
    from processors import metrics
    from processors import qualite_service
    from processors import referentiel_troncons
    from processors import reports
    from processors import storage
    from processors import traffic
    from processors import validations
    from processors import weather
    from processors.base import PipelineResult, QualityReport
    from processors.config import get_config
    from processors.storage import InputReader, OutputWriter


API_PROCESSORS = {
    "bikes": bikes.process,
    "traffic": traffic.process,
    "weather": weather.process,
}

BATCH_PROCESSORS = {
    "comptage-velo-donnees-compteurs-cleaned.csv": ("comptage_velo", comptage_velo.process),
    "chantiers-a-paris-cleaned.csv": ("chantiers", chantiers.process),
    "indicateurs-de-qualite-de-service-sncf-et-ratp.csv": ("qualite_service", qualite_service.process),
    "referentiel-geographique-pour-les-donnees-trafic-issues-des-capteurs-permanents.csv": ("referentiel_troncons", referentiel_troncons.process),
    "validations-reseau-surface-nombre-validations-par-jour-2eme-trimestre.csv": ("validations", validations.process),
}


@dataclass(slots=True)
class DailyProcessingOutput:
    api_results: Dict[str, PipelineResult]
    batch_results: Dict[str, PipelineResult]
    aggregates: Dict[str, pd.DataFrame]
    metrics_cityflow: Dict[str, any]
    correlations: Dict[str, pd.DataFrame]


def _merge_results(results: Iterable[PipelineResult]) -> PipelineResult:
    results_list = list(results)
    if not results_list:
        empty_quality = QualityReport(passed=True, messages=["info: no files found"])
        return PipelineResult(pd.DataFrame(), empty_quality, metadata={})

    frames = [res.dataframe for res in results_list if not res.dataframe.empty]
    dataframe = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    messages: List[str] = []
    passed = True
    metadata: List[Dict[str, object]] = []
    for res in results_list:
        messages.extend(res.quality_report.messages)
        passed = passed and res.quality_report.passed
        metadata.append(res.metadata)

    quality = QualityReport(passed=passed, messages=messages)
    combined_metadata = {"inputs": metadata}
    return PipelineResult(dataframe, quality, combined_metadata)


def _quality_summary(results: Dict[str, PipelineResult]) -> List[Dict[str, object]]:
    summary: List[Dict[str, object]] = []
    for name, result in results.items():
        summary.append(
            {
                "dataset": name,
                "passed": result.quality_report.passed,
                "messages": result.quality_report.messages,
                "rows": int(result.dataframe.shape[0]) if result.dataframe is not None else 0,
                "metadata": result.metadata,
            }
        )
    return summary


def _materialise_outputs(output_root: Path | None, date: str, outputs: DailyProcessingOutput, writer: OutputWriter = None) -> None:
    """
    Sauvegarde les outputs :
    - Agrégats : fichiers JSON locaux uniquement (pour compatibilité) si output_root fourni
    - Métriques, corrélations et rapports : base de données uniquement (MongoDB ou DynamoDB)
    """
    # Sauvegarder uniquement les agrégats en local si output_root est fourni
    if output_root:
        base_dir = output_root / date
        metrics_dir = base_dir / "metrics"
        metrics_dir.mkdir(parents=True, exist_ok=True)

        for name, dataframe in outputs.aggregates.items():
            if dataframe.empty:
                continue
            target = metrics_dir / f"{name}.json"
            dataframe.to_json(target, orient="records", indent=2, force_ascii=False)

    # Générer les rapports (pour la base de données)
    report_payload = {
        "date": date,
        "api": _quality_summary(outputs.api_results),
        "batch": _quality_summary(outputs.batch_results),
        "aggregates": list(outputs.aggregates.keys()),
        "metrics_cityflow": list(outputs.metrics_cityflow.keys()),
        "correlations": list(outputs.correlations.keys())
    }
    
    # Générer le rapport quotidien CityFlow
    rapport_complet = None
    df_comptage = outputs.batch_results.get("comptage_velo")
    if df_comptage and not df_comptage.dataframe.empty:
        rapport_complet = reports.generate_rapport_complet(
            df_comptage.dataframe,
            outputs.metrics_cityflow,
            date
        )
    
    # Générer le résumé des métriques
    metrics_summary = reports.generate_metrics_summary(outputs.metrics_cityflow)
    
    # Sauvegarder dans la base de données (MongoDB ou DynamoDB)
    if writer is not None:
        print("   📤 Sauvegarde dans la base de données...")
        
        # Préparer les métriques pour la base de données
        # Toutes les métriques sont maintenant des DataFrames
        metrics_for_db = {}
        for metric_name, metric_data in outputs.metrics_cityflow.items():
            if isinstance(metric_data, pd.DataFrame) and not metric_data.empty:
                if metric_name == "top_compteurs":
                    print("🔎 DEBUG top_compteurs columns:", list(metric_data.columns))
                    print(
                        "🔎 DEBUG top_compteurs sample:",
                        metric_data.head(3).to_dict(orient="records"),
                    )
                metrics_for_db[metric_name] = json.loads(metric_data.to_json(orient="records"))
        
        # Sauvegarder les métriques
        if metrics_for_db:
            success_metrics = writer.write_metrics(date, metrics_for_db)
            if success_metrics:
                print("      ✓ Métriques sauvegardées dans la base")
            else:
                print("      ⚠️  Échec sauvegarde métriques")
        
        # Préparer les corrélations pour la base de données
        correlations_for_db = {}
        for corr_name, corr_df in outputs.correlations.items():
            if not corr_df.empty:
                correlations_for_db[corr_name] = json.loads(corr_df.to_json(orient="records"))
        
        # Sauvegarder les corrélations
        if correlations_for_db:
            success_corr = writer.write_correlations(date, correlations_for_db)
            if success_corr:
                print("      ✓ Corrélations sauvegardées dans la base")
            else:
                print("      ⚠️  Échec sauvegarde corrélations")
        
        # Sauvegarder les 3 rapports dans la base de données
        all_reports = {
            "processing_report": report_payload,
            "metrics_summary": metrics_summary,
            "rapport_quotidien": rapport_complet
        }
        
        success_report = writer.write_reports(date, all_reports)
        if success_report:
            print("      ✓ 3 rapports sauvegardés dans la base (processing_report, metrics_summary, rapport_quotidien)")
        else:
            print("      ⚠️  Échec sauvegarde rapports")
    else:
        print("   ⚠️  Aucun writer fourni : métriques, corrélations et rapports non sauvegardés")


def _process_api_sources(base_api: Path, date: str, reader: InputReader = None) -> Dict[str, PipelineResult]:
    results: Dict[str, PipelineResult] = {}
    cfg = get_config()
    
    for source, processor in API_PROCESSORS.items():
        source_dir = base_api / source / date
        
        # Si on est en mode AWS et qu'on a un reader, utiliser S3
        if cfg.is_aws and reader:
            # Lister les fichiers depuis S3
            s3_prefix = f"raw/api/{source}/{date}"
            files = reader.list_files(s3_prefix)
            
            if not files:
                continue
            
            print(f"   - Traitement API: {source}... ({len(files)} fichiers depuis S3)")
            
            # Télécharger chaque fichier et le traiter
            runs = []
            for s3_file in sorted(files):
                # Extraire la clé S3 depuis le path
                s3_key = str(s3_file).replace(f"/tmp/{cfg.s3_raw_bucket}/", "")
                local_path = Path(f"/tmp/{source}_{date}_{s3_file.name}")
                
                if reader.download_if_needed(s3_key, local_path):
                    runs.append(processor(local_path))
            
            if runs:
                results[source] = _merge_results(runs)
        else:
            # Mode local : utiliser le système de fichiers
            if not source_dir.exists():
                continue
            print(f"   - Traitement API: {source}...")
            runs = [processor(path) for path in sorted(source_dir.glob("*.json"))]
            results[source] = _merge_results(runs)
    
    return results


def _process_batch_sources(base_batch: Path, date: str, reader: InputReader = None) -> Dict[str, PipelineResult]:
    results: Dict[str, PipelineResult] = {}
    cfg = get_config()
    
    # Si on est en mode AWS et qu'on a un reader, utiliser S3
    if cfg.is_aws and reader:
        for filename, (key, processor) in BATCH_PROCESSORS.items():
            # Construire la clé S3
            s3_key = f"raw/batch/{date}/{filename}"
            local_path = Path(f"/tmp/batch_{date}_{filename}")
            
            print(f"   - Traitement batch: {key} ({filename})...")
            
            # Télécharger depuis S3
            if reader.download_if_needed(s3_key, local_path):
                results[key] = processor(local_path)
            else:
                print(f"      ⚠️  Fichier {filename} non trouvé dans S3")
    else:
        # Mode local : utiliser le système de fichiers
        if not base_batch.exists():
            return results
        for filename, (key, processor) in BATCH_PROCESSORS.items():
            file_path = base_batch / filename
            if not file_path.exists():
                continue
            print(f"   - Traitement batch: {key} ({filename})...")
            results[key] = processor(file_path)
    
    return results


def _build_aggregates(
    api_results: Dict[str, PipelineResult],
    batch_results: Dict[str, PipelineResult],
) -> Dict[str, pd.DataFrame]:
    aggregates: Dict[str, pd.DataFrame] = {}

    # API bikes : données temps réel Vélib
    bikes_df = api_results.get("bikes").dataframe if api_results.get("bikes") else pd.DataFrame()
    if not bikes_df.empty:
        aggregates["velib_realtime"] = aggregation.aggregate_velib_realtime(bikes_df)

    # API traffic : incidents de circulation
    traffic_df = api_results.get("traffic").dataframe if api_results.get("traffic") else pd.DataFrame()
    if not traffic_df.empty:
        aggregates["traffic_daily"] = aggregation.aggregate_traffic_incidents(traffic_df)

    # API weather : données météo
    weather_df = api_results.get("weather").dataframe if api_results.get("weather") else pd.DataFrame()
    if not weather_df.empty:
        aggregates["weather_daily"] = aggregation.aggregate_weather_daily(weather_df)

    # Batch comptage_velo : comptages historiques de vélos
    comptage_df = batch_results.get("comptage_velo").dataframe if batch_results.get("comptage_velo") else pd.DataFrame()
    if not comptage_df.empty:
        aggregates["comptage_velo_daily"] = aggregation.aggregate_comptage_velo(comptage_df)

    validations_df = batch_results.get("validations").dataframe if batch_results.get("validations") else pd.DataFrame()
    if not validations_df.empty:
        aggregates["validations_daily"] = aggregation.aggregate_validations(validations_df)

    # Construction des KPIs combinés si on a au moins quelques agrégats
    if aggregates:
        aggregates["daily_kpis"] = aggregation.build_kpis(aggregates)

    return aggregates


def _calculate_cityflow_metrics(
    api_results: Dict[str, PipelineResult],
    batch_results: Dict[str, PipelineResult],
) -> Dict[str, any]:
    """
    Calcule toutes les métriques CityFlow Analytics.
    """
    print("\n🔬 Calcul des métriques CityFlow Analytics...")
    
    # Récupérer les DataFrames nécessaires
    comptage_df = batch_results.get("comptage_velo").dataframe if batch_results.get("comptage_velo") else pd.DataFrame()
    chantiers_df = batch_results.get("chantiers").dataframe if batch_results.get("chantiers") else pd.DataFrame()
    qualite_df = batch_results.get("qualite_service").dataframe if batch_results.get("qualite_service") else pd.DataFrame()
    geo_df = batch_results.get("referentiel_troncons").dataframe if batch_results.get("referentiel_troncons") else pd.DataFrame()
    bikes_df = api_results.get("bikes").dataframe if api_results.get("bikes") else pd.DataFrame()
    
    # Calculer toutes les métriques
    cityflow_metrics = metrics.calculate_all_metrics(
        df_comptage_velo=comptage_df,
        df_chantiers=chantiers_df,
        df_qualite=qualite_df,
        df_geo=geo_df,
        df_bikes=bikes_df
    )
    
    print(f"   ✓ {len(cityflow_metrics)} métriques CityFlow calculées")
    return cityflow_metrics


def _calculate_correlations(
    api_results: Dict[str, PipelineResult],
    batch_results: Dict[str, PipelineResult],
) -> Dict[str, pd.DataFrame]:
    """
    Calcule les corrélations entre les différentes sources de données.
    """
    print("\n🔗 Calcul des corrélations...")
    
    correlations: Dict[str, pd.DataFrame] = {}
    
    # Récupérer les DataFrames
    comptage_df = batch_results.get("comptage_velo").dataframe if batch_results.get("comptage_velo") else pd.DataFrame()
    chantiers_df = batch_results.get("chantiers").dataframe if batch_results.get("chantiers") else pd.DataFrame()
    qualite_df = batch_results.get("qualite_service").dataframe if batch_results.get("qualite_service") else pd.DataFrame()
    validations_df = batch_results.get("validations").dataframe if batch_results.get("validations") else pd.DataFrame()
    weather_df = api_results.get("weather").dataframe if api_results.get("weather") else pd.DataFrame()
    
    # Corrélation chantiers ↔ vélo
    if not comptage_df.empty and not chantiers_df.empty:
        corr_chantiers = aggregation.correlate_chantiers_velo(comptage_df, chantiers_df)
        if not corr_chantiers.empty:
            correlations["chantiers_velo"] = corr_chantiers
    
    # Corrélation qualité ↔ validations
    if not qualite_df.empty and not validations_df.empty:
        corr_qualite = aggregation.correlate_qualite_validations(qualite_df, validations_df)
        if not corr_qualite.empty:
            correlations["qualite_validations"] = corr_qualite
    
    # Corrélation météo ↔ vélo
    if not weather_df.empty and not comptage_df.empty:
        corr_meteo = aggregation.correlate_meteo_velo(weather_df, comptage_df)
        if not corr_meteo.empty:
            correlations["meteo_velo"] = corr_meteo
    
    print(f"   ✓ {len(correlations)} corrélations calculées")
    return correlations


def process_day(raw_root: Path, date: str, *, output_root: Path | None = None, writer: OutputWriter | None = None, reader: InputReader | None = None) -> DailyProcessingOutput:
    base_api = raw_root / "raw" / "api"
    base_batch = raw_root / "raw" / "batch" / date

    print("📡 Traitement des sources API...")
    api_results = _process_api_sources(base_api, date, reader)
    print(f"   ✓ {len(api_results)} sources API traitées")
    
    print("\n📦 Traitement des sources batch...")
    batch_results = _process_batch_sources(base_batch, date, reader)
    print(f"   ✓ {len(batch_results)} sources batch traitées")
    
    print("\n📊 Agrégation des données...")
    aggregates = _build_aggregates(api_results, batch_results)
    print(f"   ✓ {len(aggregates)} agrégats créés")

    # NOUVEAU : Calcul des métriques CityFlow Analytics
    cityflow_metrics = _calculate_cityflow_metrics(api_results, batch_results)
    
    # NOUVEAU : Calcul des corrélations
    correlations = _calculate_correlations(api_results, batch_results)

    output = DailyProcessingOutput(
        api_results=api_results,
        batch_results=batch_results,
        aggregates=aggregates,
        metrics_cityflow=cityflow_metrics,
        correlations=correlations
    )

    # Toujours sauvegarder (agrégats en local si output_root fourni, métriques en DB si writer fourni)
    print(f"\n💾 Sauvegarde des résultats...")
    _materialise_outputs(output_root, date, output, writer=writer)
    if output_root is not None:
        print("   ✓ Agrégats sauvegardés localement")
    if writer is not None:
        print("   ✓ Métriques, corrélations et rapports sauvegardés dans la base de données")

    return output


def _print_summary(output: DailyProcessingOutput, date: str, output_root: Path | None) -> None:
    print(f"\n{'='*80}")
    print(f"📊 RÉSUMÉ DU TRAITEMENT - {date}")
    print(f"{'='*80}")
    
    print("\n📡 Sources API traitées:")
    for source in output.api_results.keys():
        rows = len(output.api_results[source].dataframe)
        print(f"   • {source}: {rows} enregistrements")
    
    print("\n📦 Sources batch traitées:")
    for source in output.batch_results.keys():
        rows = len(output.batch_results[source].dataframe)
        print(f"   • {source}: {rows} enregistrements")
    
    print("\n📊 Agrégations générées:")
    for agg_name in output.aggregates.keys():
        rows = len(output.aggregates[agg_name])
        print(f"   • {agg_name}: {rows} lignes")
    
    print("\n🔬 Métriques CityFlow Analytics:")
    for metric_name in output.metrics_cityflow.keys():
        print(f"   • {metric_name}")
    
    print("\n🔗 Corrélations calculées:")
    if output.correlations:
        for corr_name in output.correlations.keys():
            rows = len(output.correlations[corr_name])
            print(f"   • {corr_name}: {rows} lignes")
    else:
        print("   • Aucune corrélation calculée")
    
    if output_root:
        print(f"\n📂 Fichiers locaux générés:")
        print(f"   • Agrégats uniquement : {output_root / date / 'metrics'}")
    
    print(f"\n💾 Données en base de données:")
    print(f"   • Métriques CityFlow : collection 'cityflow-metrics'")
    print(f"   • Corrélations : collection 'cityflow-daily-correlations'")
    print(f"   • Rapports (processing, quotidien, summary) : collection 'cityflow-daily-reports'")
    
    print(f"\n{'='*80}")


def _list_available_dates(raw_root: Path) -> List[str]:
    dates: set[str] = set()
    batch_root = raw_root / "raw" / "batch"
    if batch_root.exists():
        dates.update(p.name for p in batch_root.iterdir() if p.is_dir())

    api_root = raw_root / "raw" / "api"
    if api_root.exists():
        for source_dir in api_root.iterdir():
            if not source_dir.is_dir():
                continue
            dates.update(p.name for p in source_dir.iterdir() if p.is_dir())

    return sorted(dates)


def _resolve_processing_date(raw_root: Path, desired: str) -> str:
    available = _list_available_dates(raw_root)
    if desired in available:
        return desired
    if available:
        return available[-1]
    return desired


def _detect_raw_root() -> Path:
    candidates = [
        Path("."),
        Path("bucket-cityflow-paris-s3-raw"),
    ]
    for candidate in candidates:
        if (candidate / "raw").exists():
            return candidate
    return Path(".")


if __name__ == "__main__":
    # Charger la configuration
    cfg = get_config()
    
    print("="*80)
    print("🚀 CITYFLOW ANALYTICS - TRAITEMENT DES DONNÉES")
    print("="*80)
    print(f"📌 Environnement : {cfg.environment.upper()}")
    print(f"📥 Source Input  : {cfg.input_source}")
    print(f"📤 Cible Output  : {cfg.output_target}")
    print("="*80)
    
    # Initialiser le lecteur et l'écrivain
    reader = InputReader()
    writer = OutputWriter()
    
    print("\n🔍 Détection du répertoire raw...")
    raw_root = reader.get_raw_root()
    print(f"   ✓ Répertoire raw: {raw_root}")
    
    print("\n📅 Détermination de la date de traitement...")
    today = pd.Timestamp.utcnow().strftime("%Y-%m-%d")
    requested_date = None
    if len(sys.argv) > 1 and sys.argv[1]:
        requested_date = sys.argv[1]
    elif os.getenv("PROCESSING_DATE"):
        requested_date = os.getenv("PROCESSING_DATE")
    
    if requested_date:
        print(f"   • Date demandée: {requested_date}")
    
    # En mode AWS, ne pas sauvegarder localement (manque d'espace disque)
    output_root = Path("output") if cfg.is_local else None

    base_date = requested_date or today
    target_date = _resolve_processing_date(raw_root, base_date)
    if target_date != base_date:
        if requested_date:
            print(
                f"   ⚠ Données introuvables pour {requested_date}, utilisation de la dernière date "
                f"disponible {target_date}."
            )
        else:
            print(f"   ⚠ Aucune donnée pour {today}, traitement de la dernière date disponible {target_date}.")
    else:
        print(f"   ✓ Date de traitement: {target_date}")

    print(f"\n🚀 Démarrage du traitement pour {target_date}...\n")
    try:
        output = process_day(raw_root=raw_root, date=target_date, output_root=output_root, writer=writer, reader=reader)
        print("\n✅ Traitement terminé avec succès!")
        _print_summary(output, target_date, output_root)
    except Exception as e:
        print(f"\n❌ Erreur pendant le traitement: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        # Fermer les connexions
        writer.close()
