FROM python:3.11-slim
WORKDIR /app
COPY pyproject.toml /app/
RUN pip install --no-cache-dir uv && uv pip install -r <(uv pip compile pyproject.toml)  # or poetry/pip-tools
COPY . /app
EXPOSE 8000
CMD ["python","-m","uvicorn","apps.api.main:app","--host","0.0.0.0","--port","8000"]