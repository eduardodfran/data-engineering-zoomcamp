FROM python:3.13.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin



RUN pip install pandas pyarrow

WORKDIR /code
ENV PATH="/code/.venv/bin:${PATH}"

COPY pyproject.toml .python-version uv.lock ./
RUN uv sync --locked && uv pip list

COPY ingest_data.py app.py ./

# Use absolute path since ENV PATH isn't always picked up by CMD
CMD ["/code/.venv/bin/streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]