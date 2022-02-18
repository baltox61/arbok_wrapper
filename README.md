# Arbok Wrapper
This is a decorator (wrapper function) for Colossus models (Colossus as in the analytics engine, not Mathematica). This wrapper will be used as a way to standardize how input data is received, modified, logged and monitored. This will be used for all models to ensure all models have the same input and output structure. Any new or existing models will have to add this to their file, usually this will be in an init file.

Installation To install you can do as follows:


python -m pip install git+https://git.enova.com/analytics-platform/colossus.git -U
How to add to you requirements.txt:

git+https://git.enova.com/analytics-platform/colossus.git@main#egg=arbok_wrapper
How to add to your code To use the arobok_wrapper decorator you need to first install and then import into your file: from arbok_wrapper import arbok_wrapper

You then need to add the wrapper above ur predict function:


@arbok_wrapper
def predict_fn(inputs, model):
    return({'result': funcToRunModel(**inputs)})
