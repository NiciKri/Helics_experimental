import pandas as pd
import numpy as np

def load_solar_data(
    data_dir: str,
    scaling_factor: float = 1.0,
    start_time: int = 0
) -> pd.DataFrame:
    # 1. Read & clean up
    solar_data = pd.read_csv(f"{data_dir}/solar_data.csv")
    solar_data.columns = solar_data.columns.str.replace('_pv$', '', regex=True)
    solar_data.columns = solar_data.columns.str.replace('S', 's')
    
    # 2. Drop first `start_time` rows (if any), reset index
    if start_time > 0:
        solar_data = solar_data.iloc[start_time:].reset_index(drop=True)
    
    # 3. Rebuild time column from the new index
    solar_data['time'] = solar_data.index
    
    # 4. Scale all numeric cols except 'time'
    for col in solar_data.select_dtypes(include=[np.number]).columns:
        if col != 'time':
            solar_data[col] *= scaling_factor

    return solar_data


def load_load_data(
    data_dir: str,
    scaling_factor: float = 1.0,
    start_time: int = 0
) -> pd.DataFrame:
    # 1. Read & clean up
    load_data = pd.read_csv(f"{data_dir}/load_data.csv")
    load_data.columns = load_data.columns.str.replace('S', 's')
    
    # 2. Drop first `start_time` rows (if any), reset index
    if start_time > 0:
        load_data = load_data.iloc[start_time:].reset_index(drop=True)
    
    # 3. Rebuild time column from the new index
    load_data['time'] = load_data.index
    load_data.sort_values('time', inplace=True)
    
    # 4. Scale all numeric cols except 'time'
    for col in load_data.select_dtypes(include=[np.number]).columns:
        if col != 'time':
            load_data[col] *= scaling_factor

    return load_data


def load_breaking_points(data_dir: str) -> pd.DataFrame:
    breaking_points = pd.read_csv(f"{data_dir}/solar_VV_breakpoints.csv")
    breaking_points.columns = breaking_points.columns.str.replace('_pv$', '', regex=True)
    breaking_points.columns = breaking_points.columns.str.replace('S', 's')
    return breaking_points
