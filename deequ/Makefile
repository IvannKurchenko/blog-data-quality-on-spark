DATA_DIR = data_faa
ZIP_FILE = $(DATA_DIR)/ReleasableAircraft.zip
FAA_URL = https://registry.faa.gov/database/ReleasableAircraft.zip

setup:
	@echo "[SETUP] Setup complete"

clean-faa:
	@echo "[CLEAN] Removing directory: $(DATA_DIR)"
	@rm -rf $(DATA_DIR)
	@echo "[CLEAN] Cleanup complete"

install:
	@echo "[INSTALL] Compiling with sbt..."
	@sbt compile
	@echo "[INSTALL] Compilation complete"

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
	@echo "[FORMAT] Formatting code with scalafmt..."
	@sbt scalafmt
	@echo "[FORMAT] Code formatted successfully"

check:
	@echo "[CHECK] Running scalafmt checks..."
	@sbt scalafmtCheck
	@echo "[CHECK] Check complete"

run:
	@sbt run

clean:
	rm -rf target project/target

.PHONY: setup download-maria-db-driver download-faa clean-faa install format check