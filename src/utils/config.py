import yaml
import os

def load_config(config_path: str = None) -> dict:
    if config_path is None:
        # Find config relative to project root
        project_root = os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        )
        config_path = os.path.join(project_root, "config", "config.yaml")
    
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def get_full_path(config: dict, *keys) -> str:
    """Build full path from base + relative path from config"""
    base = config["paths"]["base"]
    # Navigate nested keys
    value = config["paths"]
    for key in keys:
        value = value[key]
    return os.path.join(base, value)
