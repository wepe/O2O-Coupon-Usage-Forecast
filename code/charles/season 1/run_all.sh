#!/usr/bin/env bash
#python data_view.py
#python data_split.py
python feature_extract.py
python gen_data.py
python xgb.py