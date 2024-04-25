FROM europe-docker.pkg.dev/vertex-ai/training/pytorch-gpu.1-10:latest

# Copies the trainer code to the docker image.

WORKDIR /

COPY *.py /
COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

# Sets up the entry point to invoke the trainer.
#ENTRYPOINT ["run.sh"]
ENTRYPOINT ["python", "-m", "run_model"]
