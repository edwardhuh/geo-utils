"""Helper functions that dictates the concurrent processes for geocoding processes"""

import concurrent.futures
from itertools import repeat
import pandas as pd
import numpy as np
from tqdm.auto import tqdm
import os
from typing import Dict, List, Optional, Union

from geo_utils import smartystreets_utils


def df_group_by_zip(df):
  index_name = df.columns[df.columns.str.contains('zip', case=False)]
  # collect the first in case multiple match
  df_split = list(df.groupby(index_name[0]))
  df_only_list = [df_split[i][1] for i in np.arange(len(df_split))]
  return df_only_list


def smarty_process_thread(
    df: pd.DataFrame,
    smartystreets_auth_id: str,
    smartystreets_auth_token: str
) -> pd.DataFrame:
  df_copied = df.copy()
  df_copied['smartystreets_auth_id'] = smartystreets_auth_id
  df_copied['smartystreets_auth_token'] = smartystreets_auth_token

  df_tolist = df_copied.values.tolist()
  with concurrent.futures.ThreadPoolExecutor() as executor:
    ss_results = pd.DataFrame.from_dict(
      dict(
        tqdm(
          executor.map(
            smartystreets_utils.smarty_api, df_tolist),
          total=len(df_tolist),
          desc=f'Threading in PID {os.getpid()}'
        )
      )
    )
    # Formatting columns names
    combined_output = pd.concat(
      [df.reset_index(drop=True), ss_results],
      axis=1,
      ignore_index=True)  # concat strips colnames
    combined_output.columns = list(df.columns) + list(ss_results.columns)  # combines original cols
    return combined_output


def smarty_process_pool(
    df_list: List[pd.DataFrame],
    smartystreets_auth_id: str,
    smartystreets_auth_token: str,
    max_workers: int = 3):
  with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
    results = executor.map(smarty_process_thread, df_list,
                           repeat(smartystreets_auth_id),
                           repeat(smartystreets_auth_token))
  return pd.concat(list(results)).reset_index(drop=True)
