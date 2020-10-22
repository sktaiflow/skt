# Define a builder image
FROM python:3.7 as builder

ENV DEBIAN_FRONTEND=noninteractive
ENV LANG=C.UTF-8
RUN apt-get update \
 && apt-get install -yq --no-install-recommends \
    ca-certificates \
 && pip install --upgrade pip \
 && pip install pipenv

# Python requirements
COPY Pipfile* /tmp/
RUN cd /tmp && pipenv lock --requirements > requirements.txt
RUN pip wheel -r /tmp/requirements.txt -w /wheels

# Now define the runtime image
FROM python:3.7-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV LANG=C.UTF-8

RUN apt-get update \
 && apt-get install -yq --no-install-recommends \
    ca-certificates \
 && apt-get clean && rm -rf /var/lib/apt/lists/*


COPY --from=builder /wheels /wheels
RUN python3 -mpip install --no-cache /wheels/*

# Copy source tree in
COPY . .
EXPOSE 80

# Run your program under Tini
# Add Tini https://github.com/krallin/tini
ENV TINI_VERSION v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--", "./entrypoint.sh"]