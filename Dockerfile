FROM public.ecr.aws/lambda/python:3.10

COPY requirements.txt ${LAMBDA_TASK_ROOT}

RUN pip install -r requirements.txt

COPY lambda_function.py ${LAMBDA_TASK_ROOT}

# Copy .env file
# COPY envFiles/* ./

CMD ["lambda_function.lambda_handler"]


