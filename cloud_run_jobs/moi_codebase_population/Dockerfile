FROM python:3.10-slim AS base
WORKDIR /workspace

# 安裝python所需套件
COPY ["requirements.txt","./"] 
RUN pip install --upgrade pip && pip install -r requirements.txt && pip cache purge
COPY ["main.py","script.sh","./"] 

# docker build --target dev -t gcp_cloudrun_jobs/moi_codebase_population:dev .
FROM base as dev
# credentials for dev
COPY ../key.json .
ENV GOOGLE_APPLICATION_CREDENTIALS=/workspace/key.json

# docker build --target prod -t gcp_cloudrun_jobs/moi_codebase_population:prod .
# docker tag gcp_cloudrun_jobs/moi_codebase_population:prod asia-east1-docker.pkg.dev/corgis-361708/corgis-container-repo/moi_codebase_population
FROM base as prod
CMD [ "./script.sh" ]
