import os
import yaml
import logging

logging.basicConfig(filename='experiments.log', level=logging.INFO)
logger = logging.getLogger(__name__)

# Update params.yaml with the given version and seed.
def update_params(version, seed):
    with open("params.yaml", 'r') as f:
        params = yaml.safe_load(f)
    params['data']['version'] = version
    params['data']['seed'] = seed
    with open("params.yaml", 'w') as f:
        yaml.safe_dump(params, f)
    logger.info(f"Updated params.yaml: version={version}, seed={seed}")

# Run a DVC experiment with the given version and seed.
def run_experiment(version, seed):
    exp_name = f"{version}_seed{seed}"
    logger.info(f"Running experiment: {exp_name}")
    update_params(version, seed)
    os.system(f"dvc exp run --name {exp_name}")

if __name__ == "__main__":
    versions = ["v1", "v2", "v3", "v1+v2", "v1+v2+v3"]
    seeds = [42, 123, 456]
    
    for version in versions:
        for seed in seeds:
            run_experiment(version, seed)
    
    logger.info("All experiments completed")
    print("Run 'dvc exp show' to view results")