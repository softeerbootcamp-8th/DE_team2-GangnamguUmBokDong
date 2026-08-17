import lightgbm as lgb
import numpy as np
import boto3
import io

# 38 features dummy data
X = np.zeros((1, 38))
y = np.array([1.0])

train_data = lgb.Dataset(X, label=y)

params = {
    'objective': 'regression',
    'num_leaves': 2,
    'learning_rate': 0.1,
    'min_data_in_leaf': 1
}

import os
s3 = boto3.client(
    "s3",
    endpoint_url=os.environ.get("MINIO_URL", "http://minio:9000"),
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
)
bucket = "local-dev"

for target in ["rental", "return"]:
    for suffix in ["poisson", "q10", "q50", "q90"]:
        if suffix == "poisson":
            params['objective'] = 'poisson'
        else:
            params['objective'] = 'quantile'
            params['alpha'] = float(suffix[1:]) / 100.0

        bst = lgb.train(params, train_data, num_boost_round=1)
        model_str = bst.model_to_string()
        key = f"models/{target}_{suffix}.txt"
        s3.put_object(Bucket=bucket, Key=key, Body=model_str.encode("utf-8"))
        print(f"Uploaded {key}")
    
    # Upload conformal correction dummy data
    corr_key = f"models/{target}_conformal_correction.json"
    s3.put_object(Bucket=bucket, Key=corr_key, Body=b'{"correction": 0.0}')
    print(f"Uploaded {corr_key}")
