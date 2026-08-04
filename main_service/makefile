.ONESHELL:
.SILENT:

.PHONY: build-frontend ensure-frontend
build-frontend:
	set -e; \
	cd ./frontend; \
	npm ci; \
	VITE_SYNCFUSION_LICENSE_KEY= npm run build; \
	rsync -a --delete --exclude='.*' --exclude='login.html.j2' dist/ ../src/main_service/static/; \
	cd ..; \
	printf "$$(date +%s)" > .frontend_last_built_at.txt

ensure-frontend:
	set -e; \
	if ! python3 -c 'import re, sys; from pathlib import Path; static = Path("src/main_service/static"); html = (static / "index.html").read_text(); assets = re.findall(r"(?:src|href)=\"(/assets/[^\"]+)", html); sys.exit(not assets or any(not (static / path.lstrip("/")).exists() for path in assets))'; then \
		$(MAKE) build-frontend; \
	fi
