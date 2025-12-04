DATA_DIR = src/data_faa
ZIP_FILE = $(DATA_DIR)/ReleasableAircraft.zip
FAA_URL = https://registry.faa.gov/database/ReleasableAircraft.zip
MARIADB_JAR = mariadb-java-client-3.5.6.jar
MARIADB_URL = https://repo1.maven.org/maven2/org/mariadb/jdbc/mariadb-java-client-3.5.6/mariadb-java-client-3.5.6.jar

setup:
	@echo "[SETUP] Creating virtual environment..."
	uv venv
	@echo "[SETUP] Virtual environment created successfully"

clean-faa:
	@echo "[CLEAN] Removing directory: $(DATA_DIR)"
	@rm -rf $(DATA_DIR)
	@echo "[CLEAN] Cleanup complete"

install:
	@echo "[INSTALL] Locking dependencies..."
	@uv lock
	@echo "[INSTALL] Syncing dependencies..."
	@uv sync
	@echo "[INSTALL] Installation complete"

download-faa:
	@echo "[FAA] Creating data directory: $(DATA_DIR)"
	@mkdir -p $(DATA_DIR)
	@echo "[FAA] Downloading aircraft database from $(FAA_URL)"
	@wget --timeout=60 --tries=3 \
		--user-agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36" \
		--no-check-certificate \
		-O $(ZIP_FILE) \
		$(FAA_URL)
	@echo "[FAA] Download complete. Extracting files..."
	@unzip -o $(ZIP_FILE) -d $(DATA_DIR)
	@echo "[FAA] Extraction complete. Removing zip file..."
	@rm $(ZIP_FILE)
	@echo "[FAA] Done! Files extracted to $(DATA_DIR)"

format:
	@echo "[FORMAT] Formatting code with black..."
	@black src/
	@echo "[FORMAT] Code formatted successfully"

check:
	@echo "[CHECK] Running ruff checks..."
	@ruff check src/
	@echo "[CHECK] Check complete"

clean:
	rm -rf .venv uv.lock __pycache__

.PHONY: setup download-maria-db-driver download-faa clean-faa install format check