SHELL := bash

.PHONY: help configure build test fmt check

help:
	@echo "Available targets:"
	@echo "  make configure  - Configure CMake with the dev preset"
	@echo "  make build      - Build with the dev preset"
	@echo "  make test       - Run tests from the dev preset"
	@echo "  make fmt        - Format C/C++ sources with clang-format"
	@echo "  make check      - Run fmt, build, and test"

configure:
	cmake --preset dev
	cmake -E rm -f compile_commands.json
	cmake -E create_symlink build/compile_commands.json compile_commands.json

build: configure
	cmake --build --preset dev

test: build
	ctest --preset dev

fmt:
	@command -v clang-format >/dev/null 2>&1 || { echo "clang-format not found in PATH." >&2; exit 1; }
	@set -euo pipefail; \
	files=(); \
	for dir in include src tests examples benchmarks tools; do \
	  [ -d "$$dir" ] || continue; \
	  while IFS= read -r -d '' file; do \
	    files+=("$$file"); \
	  done < <(find "$$dir" -type f \( \
	    -name '*.c' -o \
	    -name '*.cc' -o \
	    -name '*.cpp' -o \
	    -name '*.cxx' -o \
	    -name '*.h' -o \
	    -name '*.hh' -o \
	    -name '*.hpp' \
	  \) -print0); \
	done; \
	if [ "$${#files[@]}" -eq 0 ]; then \
	  echo "No C/C++ source files found to format."; \
	  exit 0; \
	fi; \
	clang-format -i "$${files[@]}"; \
	echo "Formatted $${#files[@]} file(s)."

check:
	$(MAKE) fmt
	$(MAKE) build
	$(MAKE) test
