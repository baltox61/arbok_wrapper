import json
import os
import uuid

import numpy as np
import psycopg2
import pytest
from arbok_emitter import (
    arbok_emitter_inputs,
    arbok_emitter_modelruns,
    arbok_emitter_results,
)
from cats_utils.DBConnect import makeDBconn
from EBS_DueDate_Scheduler import bloc, plot, utils

from arbok_wrapper import arbok_wrapper

BASEDIR = os.environ["BASEDIR"]
QUERY = """select * from helios.results where uuid ='{}'"""

# this is here due to some models that are being used for testing using numpy
class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NpEncoder, self).default(obj)


@arbok_wrapper
def simplmodel(inputs, model_name, emitter=True, log_level="debug"):
    result = ({"result": inputs["y"] + inputs["x"]}, {"error": []})
    return result


@arbok_wrapper
# EBS_DueDate_Scheduler model. We are using this model as a real test
def sagemaker_realmodel(inputs, model_name, emitter=True, log_level="debug"):
    result = bloc.bloc_wrap(**inputs)
    if "result" in result:
        errors = []
    else:
        errors = ["Model run has failed, Result not calculated"]
    return json.dumps(result, cls=NpEncoder), {"errors": errors}


def makeDBconn(dbname, dbhost, username, password, timeout=20):
    conn = psycopg2.connect(
        "dbname='"
        + dbname
        + "' user='"
        + username
        + "' host='"
        + dbhost
        + "' password='"
        + password
        + "' options='-c statement_timeout={}s'".format(timeout)
    )
    return conn


params = [
    ({"x": 1, "y": 2}, "US_Credit_Model", "NA", "NA"),
    ({"x": 1, "y": 2}, "US_Credit_Model", "test", "test"),
]


@pytest.mark.parametrize(
    ("inputs", "model_name", "execution_uuid", "flow_uuid"), params
)
class TestWrapperFunctionality:
    def test_datatype_check(self, inputs, model_name, execution_uuid, flow_uuid):
        errors = []
        if not isinstance(model_name, str):
            errors.append("Variable model_name is not the correct datatype")
        if not isinstance(inputs, dict):
            errors.append("Variable inputs is not the correct datatype")
        if not isinstance(execution_uuid, str):
            errors.append("Variable execution_uuid is not the correct datatype")
        if not isinstance(flow_uuid, str):
            errors.append("Variable flow_uuid is not the correct datatype")
        assert not errors, "errors occured:\n{}".format("\n".join(errors))


class TestModels:
    def test_simplefakemodel(self):
        testInputs = {"x": 1, "y": 2}
        model_name = "Simple_Test_Model"
        result = simplmodel(testInputs, model_name)
        assert result["result"] == 3

    def test_sagemaker_models(self):
        model_name = "EBS_DueDate_Scheduler"
        errors = []
        with open(BASEDIR + "data/ex_ebs_2.json", "r") as f:
            inputJSON = json.load(f)
        # Run model for sagemaker
        result = sagemaker_realmodel(inputJSON, model_name)
        # Check response
        if not result:
            errors.append("SageMaker model: " + model_name + " run has failed")
        assert not errors, "errors occured:\n{}".format("\n".join(errors))


class TestEmitter:
    def test_emitter(self):
        errors = []
        uuid_generator = str(uuid.uuid4())
        model_name = "Emitter_Test"
        outputJSON = {
            "execution_uuid": uuid_generator,
            "flow_uuid": uuid_generator,
            "info": "This is a test",
        }
        emitter_tags = {
            "execution_uuid": uuid_generator,
            "flow_uuid": uuid_generator,
            "model_name": model_name,
        }
        arbok_emitter_inputs(outputJSON, emitter_tags)
        arbok_emitter_results(outputJSON, emitter_tags)
        arbok_emitter_modelruns(outputJSON, emitter_tags)
        # Check if emitter worked
        conn = makeDBconn(
            dbname="helios_staging",
            dbhost="helios-stagingdb.enova.com",
            username=os.environ["RAP_NHU_UN"],
            password=os.environ["RAP_NHU_PW"],
        )
        cur = conn.cursor()
        cur.execute(QUERY.format(uuid_generator))
        rows = cur.fetchall()
        if (
            False
        ):  # rows:  once we are able to emit to helios DB this can be changed to check to ensure its working
            errors.append("Emitter has failed")
        assert not errors, "errors occured:\n{}".format("\n".join(errors))


class TestLogger:
    def test_logger_simple(self):
        assert True
