import json
import os
import sys

sys.path.append("/Users/mrozpadek/Colossus/analytics-platform/colossus/arbok_wrapper")
from EBS_DueDate_Scheduler import bloc, plot, utils
from arbok_wrapper import arbok_wrapper

BASEDIR = os.environ["BASEDIR"]

with open(BASEDIR + "data/ex_ebs_2.json", "r") as f:
    inputJSON = json.load(f)

# Example current sage maker model using the arbok wrapper
@arbok_wrapper
def predict_fn(inputs, model_name):
    print(inputs)
    return {"result": str(bloc.bloc_wrap(**inputs)), "model_name": model_name}, {
        "err": "test error"
    }


# this bottom step will not be necessary in an actual packaged model, this is just for easy demonstration purposes

predict_fn(inputs=inputJSON, model_name="test")
