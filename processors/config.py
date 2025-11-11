"""
Configuration management for CityFlow Analytics.
Gère les variables d'environnement pour local vs AWS.
"""

import os
from pathlib import Path
from typing import Literal
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()


class Config:
    """Configuration centrale basée sur les variables d'environnement."""
    
    def __init__(self):
        self.environment: Literal["local", "aws"] = os.getenv("ENVIRONNEMENT", "local")
        
        # Configuration AWS
        self.aws_region = os.getenv("AWS_REGION", "eu-west-3")
        
        # Configuration MongoDB (local)
        self.mongodb_url = os.getenv("MONGODB_URL", "mongodb://localhost:27017/")
        self.mongodb_database = os.getenv("MONGODB_DATABASE", "cityflow-db")
        
        # Tables de métriques (DynamoDB en AWS, collections MongoDB en local)
        self.metrics_flux_table = os.getenv("METRICS_FLUX_TABLE", "cityflow-metrics-flux")
        self.metrics_performance_table = os.getenv("METRICS_PERFORMANCE_TABLE", "cityflow-metrics-performance")
        self.metrics_analyse_table = os.getenv("METRICS_ANALYSE_TABLE", "cityflow-metrics-analyse")
        self.metrics_infrastructure_table = os.getenv("METRICS_INFRASTRUCTURE_TABLE", "cityflow-metrics-infrastructure")
        
        # Tables corrélations et rapports
        self.correlations_table = os.getenv("CORRELATIONS_TABLE", "cityflow-daily-correlations")
        self.reports_table = os.getenv("REPORTS_TABLE", "cityflow-daily-reports")
        
        # Mapping métriques → tables
        self.metrics_routing = {
            # Flux
            "debit_horaire": self.metrics_flux_table,
            "debit_journalier": self.metrics_flux_table,
            "dmja": self.metrics_flux_table,
            "evolution_hebdomadaire": self.metrics_flux_table,
            "ratio_weekend_semaine": self.metrics_flux_table,
            
            # Performance
            "taux_disponibilite": self.metrics_performance_table,
            "top_compteurs": self.metrics_performance_table,
            "compteurs_faible_activite": self.metrics_performance_table,
            "compteurs_defaillants": self.metrics_performance_table,
            "heures_pointe": self.metrics_performance_table,
            "profil_jour_type": self.metrics_performance_table,
            
            # Analyse
            "anomalies": self.metrics_analyse_table,
            "congestion_cyclable": self.metrics_analyse_table,
            "corridors_cyclables": self.metrics_analyse_table,
            "densite_par_zone": self.metrics_analyse_table,
            
            # Infrastructure
            "chantiers_actifs": self.metrics_infrastructure_table,
            "score_criticite_chantiers": self.metrics_infrastructure_table,
            "qualite_service": self.metrics_infrastructure_table,
        }
        
        # Configuration S3
        self.use_s3 = os.getenv("USE_S3", "false").lower() == "true"
        self.s3_raw_bucket = os.getenv("S3_RAW_BUCKET", "bucket-cityflow-paris-s3-raw")
        self.s3_raw_prefix = os.getenv("S3_RAW_PREFIX", "raw")
        
        # Configuration Local
        self.local_raw = os.getenv("LOCAL_RAW", "bucket-cityflow-paris-s3-raw")
        self.local_raw_prefix = os.getenv("LOCAL_RAW_PREFIX", "raw")
    
    @property
    def is_local(self) -> bool:
        """Retourne True si on est en environnement local."""
        return self.environment == "local"
    
    @property
    def is_aws(self) -> bool:
        """Retourne True si on est en environnement AWS."""
        return self.environment == "aws"
    
    @property
    def input_source(self) -> str:
        """Retourne la source d'input selon l'environnement."""
        if self.is_local:
            return f"Local: {self.local_raw}/{self.local_raw_prefix}"
        else:
            return f"S3: s3://{self.s3_raw_bucket}/{self.s3_raw_prefix}"
    
    @property
    def output_target(self) -> str:
        """Retourne la cible d'output selon l'environnement."""
        if self.is_local:
            return f"MongoDB: {self.mongodb_url}{self.mongodb_database}"
        else:
            return f"DynamoDB: {self.aws_region}"
    
    def __repr__(self) -> str:
        return (
            f"Config(environment={self.environment}, "
            f"input={self.input_source}, "
            f"output={self.output_target})"
        )


# Instance globale de configuration
config = Config()


def get_config() -> Config:
    """Retourne l'instance de configuration globale."""
    return config

