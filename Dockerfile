FROM public.ecr.aws/lambda/python:3.13

WORKDIR ${LAMBDA_TASK_ROOT}
# Copy requirements.txt
COPY requirements.txt ${LAMBDA_TASK_ROOT}


RUN pip install -r requirements.txt

COPY src/__init__.py  src/
COPY src/ingestion/ src/ingestion/
COPY src/utils/ src/utils/

COPY jobs/ingestion_jobs/ ingestion_jobs/


CMD [ "ingestion_jobs.movies_job.lambda_handler" ]