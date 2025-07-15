#!/usr/bin/env python3
"""
==============================================================================
Marine Pollution Monitoring System - Checkpoint Recovery Test
==============================================================================
Script per testare il recovery dai checkpoint Flink:
1. Avvia i job Flink e monitora l'esecuzione
2. Simula un crash (kill -9) di un task manager
3. Verifica il recovery corretto dal checkpoint più recente
4. Genera report sulla resilienza del sistema
"""

import os
import sys
import time
import json
import argparse
import subprocess
import requests
import logging
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from tabulate import tabulate

# Configurazione logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Configurazione costanti
FLINK_REST_PORT = 8081
PROMETHEUS_PORT = 9090
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
COMPONENTS = [
    "sensor_analyzer",
    "image_standardizer",
    "pollution_detector",
    "ml_prediction"
]

class CheckpointTester:
    """Tester per il recovery dai checkpoint Flink"""
    
    def __init__(self, flink_host="localhost", flink_port=FLINK_REST_PORT):
        self.flink_host = flink_host
        self.flink_port = flink_port
        self.flink_url = f"http://{flink_host}:{flink_port}"
        self.results = {
            "tests": [],
            "summary": {
                "total_tests": 0,
                "passed": 0,
                "failed": 0,
                "avg_recovery_time": 0,
                "checkpoint_success_rate": 0
            }
        }
    
    def get_jobs(self):
        """Ottiene lista dei job Flink attivi"""
        try:
            response = requests.get(f"{self.flink_url}/jobs/overview")
            response.raise_for_status()
            return response.json().get("jobs", [])
        except Exception as e:
            logger.error(f"Errore nel recupero dei job Flink: {e}")
            return []
    
    def get_job_details(self, job_id):
        """Ottiene dettagli di un job specifico"""
        try:
            response = requests.get(f"{self.flink_url}/jobs/{job_id}")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Errore nel recupero dei dettagli del job {job_id}: {e}")
            return {}
    
    def get_job_metrics(self, job_id):
        """Ottiene metriche per un job specifico"""
        try:
            response = requests.get(f"{self.flink_url}/jobs/{job_id}/metrics")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Errore nel recupero delle metriche del job {job_id}: {e}")
            return {}
    
    def get_checkpoints(self, job_id):
        """Ottiene informazioni sui checkpoint di un job"""
        try:
            response = requests.get(f"{self.flink_url}/jobs/{job_id}/checkpoints")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Errore nel recupero dei checkpoint del job {job_id}: {e}")
            return {}
    
    def get_taskmanagers(self):
        """Ottiene lista dei task manager"""
        try:
            response = requests.get(f"{self.flink_url}/taskmanagers")
            response.raise_for_status()
            return response.json().get("taskmanagers", [])
        except Exception as e:
            logger.error(f"Errore nel recupero dei task manager: {e}")
            return []
    
    def simulate_crash(self, taskmanager_id=None):
        """Simula un crash di un task manager"""
        try:
            # Se non è specificato un ID, seleziona casualmente un task manager
            if not taskmanager_id:
                taskmanagers = self.get_taskmanagers()
                if not taskmanagers:
                    logger.error("Nessun task manager disponibile")
                    return False
                taskmanager_id = taskmanagers[0].get("id")
            
            # Ottieni il container ID dal task manager ID
            container_id = self._get_container_id_from_taskmanager(taskmanager_id)
            if not container_id:
                logger.error(f"Impossibile trovare il container per il task manager {taskmanager_id}")
                return False
            
            # Esegui kill -9 sul container
            logger.info(f"Simulazione crash del task manager {taskmanager_id} (container {container_id})")
            subprocess.run(["docker", "kill", "--signal=SIGKILL", container_id], check=True)
            return True
        except Exception as e:
            logger.error(f"Errore nella simulazione del crash: {e}")
            return False
    
    def _get_container_id_from_taskmanager(self, taskmanager_id):
        """Ottiene l'ID del container Docker dal task manager ID"""
        try:
            # Esegui docker ps per ottenere i container Flink
            result = subprocess.run(
                ["docker", "ps", "--format", "{{.ID}}\t{{.Names}}", "--filter", "name=taskmanager"],
                capture_output=True, text=True, check=True
            )
            
            # Cerca il container con il task manager ID
            for line in result.stdout.splitlines():
                container_id, name = line.split("\t")
                if taskmanager_id in name:
                    return container_id
            
            # Se non trovato, seleziona il primo container taskmanager
            for line in result.stdout.splitlines():
                if "taskmanager" in line:
                    container_id = line.split("\t")[0]
                    return container_id
            
            return None
        except Exception as e:
            logger.error(f"Errore nel recupero del container ID: {e}")
            return None
    
    def monitor_recovery(self, job_id, timeout=300):
        """Monitora il recovery di un job dopo un crash"""
        start_time = time.time()
        end_time = start_time + timeout
        recovered = False
        recovery_time = None
        
        logger.info(f"Monitoraggio recovery del job {job_id}...")
        
        # Memorizza stato job prima del crash
        pre_crash_status = self.get_job_details(job_id).get("state", "UNKNOWN")
        
        while time.time() < end_time:
            try:
                # Controlla stato job
                job_details = self.get_job_details(job_id)
                current_status = job_details.get("state", "UNKNOWN")
                
                logger.info(f"Stato attuale del job: {current_status}")
                
                # Se il job è tornato allo stato originale (RUNNING), consideriamo il recovery completato
                if current_status == "RUNNING" and current_status == pre_crash_status:
                    recovered = True
                    recovery_time = time.time() - start_time
                    logger.info(f"Recovery completato in {recovery_time:.2f} secondi")
                    break
                
                # Se il job è fallito definitivamente, interrompi il monitoraggio
                if current_status in ["FAILED", "CANCELED"]:
                    logger.error(f"Il job è fallito con stato {current_status}")
                    break
                
                # Aspetta prima del prossimo controllo
                time.sleep(5)
                
            except Exception as e:
                logger.warning(f"Errore durante il monitoraggio: {e}")
                time.sleep(5)
        
        if not recovered:
            logger.error(f"Timeout: il job non si è ripreso dopo {timeout} secondi")
        
        return {
            "recovered": recovered,
            "recovery_time": recovery_time,
            "final_status": self.get_job_details(job_id).get("state", "UNKNOWN")
        }
    
    def verify_data_integrity(self, job_id):
        """Verifica l'integrità dei dati dopo il recovery"""
        try:
            # Ottieni informazioni dai topic Kafka per verificare la consistenza
            # Questo richiede l'implementazione specifica per ogni job
            
            # Esempio: verifica che i messaggi continuino ad essere processati
            # dopo il recovery confrontando i contatori pre e post crash
            
            logger.info(f"Verifica integrità dati per il job {job_id}...")
            
            # Implementazione semplificata: controlla che il job continui a processare record
            pre_metrics = self.get_job_metrics(job_id)
            time.sleep(10)  # Attendi che vengano processati nuovi record
            post_metrics = self.get_job_metrics(job_id)
            
            # Controlla se sono stati processati nuovi record
            records_before = self._get_records_metric(pre_metrics)
            records_after = self._get_records_metric(post_metrics)
            
            data_intact = records_after > records_before
            
            logger.info(f"Verifica integrità dati: {'PASSATA' if data_intact else 'FALLITA'}")
            return data_intact
        except Exception as e:
            logger.error(f"Errore nella verifica dell'integrità dei dati: {e}")
            return False
    
    def _get_records_metric(self, metrics):
        """Estrae la metrica dei record processati"""
        for metric in metrics:
            if "numRecordsIn" in metric.get("id", ""):
                return metric.get("value", 0)
        return 0
    
    def test_job_recovery(self, job_id, job_name):
        """Esegue un test completo di recovery per un job"""
        logger.info(f"=== Test di recovery per il job {job_name} ({job_id}) ===")
        
        test_result = {
            "job_id": job_id,
            "job_name": job_name,
            "timestamp": datetime.now().isoformat(),
            "checkpoint_info": {},
            "recovery_info": {},
            "data_integrity": False,
            "passed": False
        }
        
        # Ottieni informazioni sui checkpoint prima del crash
        checkpoint_info = self.get_checkpoints(job_id)
        test_result["checkpoint_info"] = {
            "count": checkpoint_info.get("counts", {}).get("completed", 0),
            "latest": checkpoint_info.get("latest", {}).get("completed", {}).get("external_path", "N/A"),
            "interval": checkpoint_info.get("checkpoint_config", {}).get("interval", 0)
        }
        
        # Simula crash
        if not self.simulate_crash():
            logger.error("Impossibile simulare il crash, test annullato")
            test_result["error"] = "Simulazione crash fallita"
            self.results["tests"].append(test_result)
            return False
        
        # Monitora recovery
        recovery_result = self.monitor_recovery(job_id)
        test_result["recovery_info"] = recovery_result
        
        # Verifica integrità dati solo se il recovery è avvenuto con successo
        if recovery_result["recovered"]:
            test_result["data_integrity"] = self.verify_data_integrity(job_id)
            test_result["passed"] = test_result["data_integrity"]
        
        # Aggiungi risultato al report
        self.results["tests"].append(test_result)
        
        logger.info(f"Test completato: {'PASSATO' if test_result['passed'] else 'FALLITO'}")
        return test_result["passed"]
    
    def run_all_tests(self):
        """Esegue i test su tutti i job Flink attivi"""
        jobs = self.get_jobs()
        
        if not jobs:
            logger.error("Nessun job Flink attivo trovato")
            return False
        
        logger.info(f"Trovati {len(jobs)} job Flink attivi")
        
        # Esegui test per ogni job
        for job in jobs:
            job_id = job.get("jid")
            job_name = job.get("name")
            
            if not job_id or not job_name:
                continue
            
            self.test_job_recovery(job_id, job_name)
        
        # Calcola statistiche di riepilogo
        self._calculate_summary()
        
        return True
    
    def _calculate_summary(self):
        """Calcola statistiche di riepilogo dai test"""
        total_tests = len(self.results["tests"])
        passed_tests = sum(1 for test in self.results["tests"] if test.get("passed", False))
        recovery_times = [test.get("recovery_info", {}).get("recovery_time", 0) 
                         for test in self.results["tests"] 
                         if test.get("recovery_info", {}).get("recovered", False)]
        
        avg_recovery_time = sum(recovery_times) / len(recovery_times) if recovery_times else 0
        
        # Calcola tasso di successo dei checkpoint
        checkpoint_success = 0
        checkpoint_total = 0
        
        for test in self.results["tests"]:
            checkpoint_info = test.get("checkpoint_info", {})
            completed = checkpoint_info.get("count", 0)
            if "counts" in test.get("checkpoint_info", {}):
                total = sum(test["checkpoint_info"]["counts"].values())
                if total > 0:
                    checkpoint_success += completed
                    checkpoint_total += total
        
        checkpoint_success_rate = (checkpoint_success / checkpoint_total * 100) if checkpoint_total > 0 else 0
        
        self.results["summary"] = {
            "total_tests": total_tests,
            "passed": passed_tests,
            "failed": total_tests - passed_tests,
            "avg_recovery_time": avg_recovery_time,
            "checkpoint_success_rate": checkpoint_success_rate
        }
    
    def generate_report(self, output_file=None):
        """Genera un report dettagliato dei test"""
        # Calcola statistiche se non già fatto
        if self.results["summary"]["total_tests"] == 0:
            self._calculate_summary()
        
        # Crea report testuale
        report = []
        report.append("==== REPORT TEST DI RECOVERY CHECKPOINT FLINK ====")
        report.append(f"Data: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Flink JobManager: {self.flink_host}:{self.flink_port}")
        report.append("")
        
        # Aggiungi riepilogo
        report.append("=== RIEPILOGO ===")
        report.append(f"Test totali: {self.results['summary']['total_tests']}")
        report.append(f"Test passati: {self.results['summary']['passed']} ({self.results['summary']['passed']/self.results['summary']['total_tests']*100:.1f}% se total_tests > 0 else 'N/A')")
        report.append(f"Test falliti: {self.results['summary']['failed']}")
        report.append(f"Tempo medio di recovery: {self.results['summary']['avg_recovery_time']:.2f} secondi")
        report.append(f"Tasso di successo checkpoint: {self.results['summary']['checkpoint_success_rate']:.1f}%")
        report.append("")
        
        # Aggiungi dettagli per ogni test
        report.append("=== DETTAGLI TEST ===")
        for i, test in enumerate(self.results["tests"], 1):
            report.append(f"Test #{i}: {test['job_name']} ({test['job_id']})")
            report.append(f"  Risultato: {'PASSATO' if test['passed'] else 'FALLITO'}")
            report.append(f"  Recovery: {'Completato' if test.get('recovery_info', {}).get('recovered', False) else 'Fallito'}")
            recovery_time = test.get('recovery_info', {}).get('recovery_time')
            report.append(f"  Tempo di recovery: {f'{recovery_time:.2f} secondi' if recovery_time else 'N/A'}")
            report.append(f"  Integrità dati: {'Verificata' if test.get('data_integrity', False) else 'Non verificata'}")
            report.append("")
        
        # Salva report su file se specificato
        if output_file:
            with open(output_file, 'w') as f:
                f.write('\n'.join(report))
            logger.info(f"Report salvato su {output_file}")
        
        # Stampa report su console
        print('\n'.join(report))
        
        # Genera anche grafici se matplotlib è disponibile
        try:
            self._generate_charts(output_file.replace('.txt', '') if output_file else None)
        except Exception as e:
            logger.warning(f"Impossibile generare grafici: {e}")
        
        return '\n'.join(report)
    
    def _generate_charts(self, output_prefix=None):
        """Genera grafici dai risultati dei test"""
        # Crea dataframe dai risultati
        data = []
        for test in self.results["tests"]:
            data.append({
                "job_name": test["job_name"],
                "recovered": test.get("recovery_info", {}).get("recovered", False),
                "recovery_time": test.get("recovery_info", {}).get("recovery_time", 0),
                "data_integrity": test.get("data_integrity", False),
                "passed": test.get("passed", False)
            })
        
        df = pd.DataFrame(data)
        
        # Grafico 1: Tempi di recovery per job
        plt.figure(figsize=(10, 6))
        plt.bar(df["job_name"], df["recovery_time"])
        plt.title("Tempi di Recovery per Job")
        plt.xlabel("Job")
        plt.ylabel("Tempo (secondi)")
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        
        if output_prefix:
            plt.savefig(f"{output_prefix}_recovery_times.png")
        else:
            plt.show()
        
        # Grafico 2: Percentuale di successo
        plt.figure(figsize=(8, 8))
        labels = ['Passati', 'Falliti']
        sizes = [self.results["summary"]["passed"], self.results["summary"]["failed"]]
        colors = ['#4CAF50', '#F44336']
        plt.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=90)
        plt.title("Risultati Test di Recovery")
        plt.axis('equal')
        
        if output_prefix:
            plt.savefig(f"{output_prefix}_success_rate.png")
        else:
            plt.show()


def main():
    """Funzione principale"""
    parser = argparse.ArgumentParser(description="Test di recovery dai checkpoint Flink")
    parser.add_argument("--host", default="localhost", help="Host del JobManager Flink")
    parser.add_argument("--port", type=int, default=FLINK_REST_PORT, help="Porta REST del JobManager Flink")
    parser.add_argument("--job-id", help="ID del job Flink da testare (se non specificato, testa tutti i job)")
    parser.add_argument("--output", help="File di output per il report")
    args = parser.parse_args()
    
    tester = CheckpointTester(args.host, args.port)
    
    if args.job_id:
        # Testa solo il job specificato
        job_details = tester.get_job_details(args.job_id)
        if not job_details:
            logger.error(f"Job {args.job_id} non trovato")
            return 1
        
        job_name = job_details.get("name", "Unknown")
        tester.test_job_recovery(args.job_id, job_name)
    else:
        # Testa tutti i job
        tester.run_all_tests()
    
    # Genera report
    tester.generate_report(args.output)
    
    # Restituisci stato di uscita in base ai risultati
    return 0 if tester.results["summary"]["failed"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())