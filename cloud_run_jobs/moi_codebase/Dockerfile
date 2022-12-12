FROM python:3.10-slim AS base
WORKDIR /workspace

#COPY ["rarlinux-x64-620b2.tar.gz","key.json","requirements.txt","./"] 
COPY ["rarlinux-x64-620b2.tar.gz","requirements.txt","./"] 
# 安裝解壓縮rar的工具
# Source file: https://www.rarlab.com/download.htm 
RUN apt-get update -y \
    && apt-get install make \
    && tar -xzpvf rarlinux-x64-620b2.tar.gz \
    && cd rar \
    && make \
    && cd .. \
    && rm rarlinux-x64-620b2.tar.gz \
    && rm -rf rar \
    && apt-get purge make -y  
# 安裝python所需套件
RUN pip install --upgrade pip && pip install -r requirements.txt && pip cache purge
COPY ["main.py","script.sh","./"] 

# docker build --target dev -t gcp_cloudrun_jobs/moi_codebase:dev .
FROM base as dev
# credentials for dev
COPY ../key.json .
ENV GOOGLE_APPLICATION_CREDENTIALS=/workspace/key.json

# docker build --target prod -t gcp_cloudrun_jobs/moi_codebase:prod .
# docker tag gcp_cloudrun_jobs/moi_codebase:prod asia-east1-docker.pkg.dev/corgis-361708/corgis-container-repo/moi_codebase
FROM base as prod
CMD [ "./script.sh" ]
