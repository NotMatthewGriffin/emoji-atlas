FROM python:3.9-slim
RUN pip install slack-bolt vaderSentiment
COPY db.py emoji_atlas.py views.py app/
WORKDIR app
ENTRYPOINT python3 emoji_atlas.py
