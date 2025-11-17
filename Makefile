setup:
	uv venv

download_maria_db_driver:
	cd src
	wget https://repo1.maven.org/maven2/org/mariadb/jdbc/mariadb-java-client-3.5.6/mariadb-java-client-3.5.6.jar

install:
	uv lock && uv sync

format:
	black src/

check:
	ruff check src/
