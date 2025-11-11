"""
Storage abstraction layer for CityFlow Analytics.
Gère les lectures (local/S3) et écritures (MongoDB/DynamoDB).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Any
from datetime import datetime

try:
    from .config import get_config
except ImportError:
    from config import get_config


class InputReader:
    """Lecteur de données qui s'adapte à l'environnement (local ou S3)."""
    
    def __init__(self):
        self.config = get_config()
        self._s3_client = None
    
    def get_raw_root(self) -> Path:
        """
        Retourne le chemin racine des données raw selon l'environnement.
        
        En local: retourne le Path du dossier local
        En AWS: retourne un Path virtuel (sera géré par S3)
        """
        if self.config.is_local:
            return Path(self.config.local_raw)
        else:
            # En AWS, on retourne un Path virtuel, les fichiers seront téléchargés depuis S3
            return Path(f"/tmp/{self.config.s3_raw_bucket}")
    
    def list_files(self, prefix: str) -> List[Path]:
        """
        Liste les fichiers avec un préfixe donné.
        
        Args:
            prefix: Préfixe du chemin (ex: "raw/api/bikes/2025-11-11")
        
        Returns:
            Liste de Path des fichiers trouvés
        """
        if self.config.is_local:
            return self._list_local_files(prefix)
        else:
            return self._list_s3_files(prefix)
    
    def _list_local_files(self, prefix: str) -> List[Path]:
        """Liste les fichiers locaux."""
        base_path = Path(self.config.local_raw) / prefix
        if not base_path.exists():
            return []
        
        files = []
        if base_path.is_dir():
            files = list(base_path.rglob("*"))
            files = [f for f in files if f.is_file()]
        
        return files
    
    def _list_s3_files(self, prefix: str) -> List[Path]:
        """Liste les fichiers depuis S3."""
        import boto3
        
        if self._s3_client is None:
            self._s3_client = boto3.client('s3', region_name=self.config.aws_region)
        
        # Le préfixe contient déjà "raw/" donc on ne l'ajoute pas
        s3_prefix = prefix
        
        try:
            response = self._s3_client.list_objects_v2(
                Bucket=self.config.s3_raw_bucket,
                Prefix=s3_prefix
            )
            
            files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    # Créer un Path virtuel
                    local_path = Path(f"/tmp/{self.config.s3_raw_bucket}") / obj['Key']
                    files.append(local_path)
            
            return files
        except Exception as e:
            print(f"⚠️  Erreur lors de la lecture S3: {e}")
            return []
    
    def download_if_needed(self, s3_key: str, local_path: Path) -> bool:
        """
        Télécharge un fichier depuis S3 si on est en mode AWS.
        
        Args:
            s3_key: Clé S3 du fichier
            local_path: Chemin local où télécharger
        
        Returns:
            True si le fichier est disponible localement
        """
        if self.config.is_local:
            return local_path.exists()
        
        import boto3
        
        if self._s3_client is None:
            self._s3_client = boto3.client('s3', region_name=self.config.aws_region)
        
        try:
            local_path.parent.mkdir(parents=True, exist_ok=True)
            self._s3_client.download_file(
                self.config.s3_raw_bucket,
                s3_key,
                str(local_path)
            )
            return True
        except Exception as e:
            print(f"⚠️  Erreur téléchargement S3 {s3_key}: {e}")
            return False


class OutputWriter:
    """Écrivain de données qui s'adapte à l'environnement (MongoDB ou DynamoDB)."""
    
    def __init__(self):
        self.config = get_config()
        self._mongo_client = None
        self._dynamo_resource = None
    
    def write_metrics(self, date: str, metrics: Dict[str, Any]) -> bool:
        """
        Écrit les métriques dans la base de données appropriée.
        Chaque métrique est sauvegardée dans un document séparé pour éviter
        la limite de 16MB par document MongoDB.
        
        Args:
            date: Date du traitement (YYYY-MM-DD)
            metrics: Dictionnaire des métriques à sauvegarder
        
        Returns:
            True si toutes les écritures ont réussi
        """
        success_count = 0
        total_count = len(metrics)
        
        # Sauvegarder chaque métrique individuellement
        for metric_name, metric_data in metrics.items():
            data = {
                "date": date,
                "metric_name": metric_name,
                "data": metric_data,
                "timestamp": datetime.utcnow().isoformat()
            }
            
            if self.config.is_local:
                success = self._write_to_mongodb(
                    collection=self.config.metrics_table,
                    data=data,
                    query_filter={"date": date, "metric_name": metric_name}
                )
            else:
                # Pour DynamoDB, on utilise date+metric_name comme clé composite
                item = {
                    "date": date,
                    "metric_name": metric_name,
                    "data": metric_data,
                    "timestamp": datetime.utcnow().isoformat()
                }
                success = self._write_to_dynamodb(
                    table_name=self.config.metrics_table,
                    item=item
                )
            
            if success:
                success_count += 1
        
        print(f"      → {success_count}/{total_count} métriques sauvegardées")
        return success_count > 0
    
    def write_correlations(self, date: str, correlations: Dict[str, List[Dict]]) -> bool:
        """
        Écrit les corrélations dans la base de données appropriée.
        
        Args:
            date: Date du traitement (YYYY-MM-DD)
            correlations: Dictionnaire des corrélations à sauvegarder
        
        Returns:
            True si l'écriture a réussi
        """
        if self.config.is_local:
            return self._write_to_mongodb(
                collection=self.config.correlations_table,
                data={"date": date, "correlations": correlations, "timestamp": datetime.utcnow().isoformat()}
            )
        else:
            return self._write_to_dynamodb(
                table_name=self.config.correlations_table,
                item={"date": date, "correlations": correlations, "timestamp": datetime.utcnow().isoformat()}
            )
    
    def write_reports(self, date: str, reports: Dict[str, Any]) -> bool:
        """
        Écrit les rapports dans la base de données appropriée.
        
        Args:
            date: Date du traitement (YYYY-MM-DD)
            reports: Dictionnaire des rapports à sauvegarder
        
        Returns:
            True si l'écriture a réussi
        """
        if self.config.is_local:
            return self._write_to_mongodb(
                collection=self.config.reports_table,
                data={"date": date, "reports": reports, "timestamp": datetime.utcnow().isoformat()}
            )
        else:
            return self._write_to_dynamodb(
                table_name=self.config.reports_table,
                item={"date": date, "reports": reports, "timestamp": datetime.utcnow().isoformat()}
            )
    
    def _write_to_mongodb(self, collection: str, data: Dict[str, Any], query_filter: Dict[str, Any] = None) -> bool:
        """Écrit dans MongoDB."""
        try:
            from pymongo import MongoClient
            
            if self._mongo_client is None:
                self._mongo_client = MongoClient(self.config.mongodb_url, serverSelectionTimeoutMS=5000)
            
            db = self._mongo_client[self.config.mongodb_database]
            coll = db[collection]
            
            # Upsert: remplacer si existe déjà pour cette date
            filter_query = query_filter if query_filter else {"date": data["date"]}
            result = coll.replace_one(
                filter_query,
                data,
                upsert=True
            )
            
            return result.acknowledged
        except Exception as e:
            print(f"⚠️  Erreur écriture MongoDB ({collection}): {e}")
            return False
    
    def _write_to_dynamodb(self, table_name: str, item: Dict[str, Any]) -> bool:
        """Écrit dans DynamoDB."""
        try:
            import boto3
            from decimal import Decimal
            
            if self._dynamo_resource is None:
                self._dynamo_resource = boto3.resource('dynamodb', region_name=self.config.aws_region)
            
            table = self._dynamo_resource.Table(table_name)
            
            # Convertir les float en Decimal pour DynamoDB
            item_converted = self._convert_floats_to_decimal(item)
            
            response = table.put_item(Item=item_converted)
            
            return response['ResponseMetadata']['HTTPStatusCode'] == 200
        except Exception as e:
            print(f"⚠️  Erreur écriture DynamoDB ({table_name}): {e}")
            return False
    
    def _convert_floats_to_decimal(self, obj: Any) -> Any:
        """Convertit les float en Decimal pour DynamoDB."""
        from decimal import Decimal
        
        if isinstance(obj, float):
            return Decimal(str(obj))
        elif isinstance(obj, dict):
            return {k: self._convert_floats_to_decimal(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_floats_to_decimal(item) for item in obj]
        else:
            return obj
    
    def close(self):
        """Ferme les connexions."""
        if self._mongo_client:
            self._mongo_client.close()

