IFACE     ?= can0
EXTRA     ?=

PREFIX    ?= $(HOME)/.local
BINDIR    ?= $(PREFIX)/bin
MANDIR    ?= $(PREFIX)/share/man

BIN       := target/release/mcandump
SRC       := $(shell find src -name '*.rs') Cargo.toml Cargo.lock

VERSION   := $(shell grep '^version' Cargo.toml | head -1 | sed 's/.*"\(.*\)"/\1/')

.PHONY: all build run test clean install uninstall fmt check clippy vcan man release publish help

all: help

build: $(BIN)

$(BIN): $(SRC)
	cargo build --release

run: $(BIN)
	./$(BIN) $(IFACE) $(EXTRA)

# Quick smoke test on vcan0
test: $(BIN)
	./$(BIN) vcan0 $(EXTRA)

# Set up a virtual CAN interface for testing
vcan:
	sudo modprobe vcan
	sudo ip link add dev vcan0 type vcan 2>/dev/null || true
	sudo ip link set up vcan0

# Set up a virtual CAN-FD interface for testing
vcanfd:
	sudo modprobe vcan
	sudo ip link add dev vcan0 type vcan mtu 72 2>/dev/null || true
	sudo ip link set up vcan0

fmt:
	cargo fmt

check:
	cargo check

clippy:
	cargo clippy -- -D warnings

man:
	@man ./man/mcandump.1

install: $(BIN)
	install -Dm755 $(BIN) $(BINDIR)/mcandump
	install -Dm644 man/mcandump.1 $(MANDIR)/man1/mcandump.1

uninstall:
	rm -f $(BINDIR)/mcandump
	rm -f $(MANDIR)/man1/mcandump.1

release: build
	@if git diff --quiet && git diff --cached --quiet; then \
		echo "Working tree clean — tagging v$(VERSION)"; \
	else \
		echo "error: uncommitted changes — commit first"; exit 1; \
	fi
	@if git tag | grep -q "^v$(VERSION)$$"; then \
		echo "error: tag v$(VERSION) already exists — bump version in Cargo.toml"; exit 1; \
	fi
	git tag -a v$(VERSION) -m "v$(VERSION)"
	git push --tags
	@echo "Tagged and pushed v$(VERSION) — GitHub release workflow will build binaries."
	@echo "Run 'make publish' to push to crates.io."

publish:
	cargo publish --dry-run
	@echo ""
	@echo "Dry run passed. Publishing v$(VERSION) to crates.io in 5s... (Ctrl-C to abort)"
	@sleep 5
	cargo publish

clean:
	cargo clean

help:
	@echo "mcandump Makefile"
	@echo ""
	@echo "Targets:"
	@echo "  build      Build release binary"
	@echo "  run        Run with IFACE and EXTRA"
	@echo "  test       Quick test on vcan0 (no zeroconf)"
	@echo "  vcan       Create vcan0 virtual interface (requires sudo)"
	@echo "  vcanfd     Create vcan0 with CAN-FD MTU (requires sudo)"
	@echo "  man        View the man page"
	@echo "  install    Install binary and man page to PREFIX (default: ~/.local)"
	@echo "  uninstall  Remove installed files"
	@echo "  fmt        cargo fmt"
	@echo "  check      cargo check"
	@echo "  clippy     cargo clippy"
	@echo "  clean      cargo clean"
	@echo "  release    Tag v$$VERSION, push tag, trigger GitHub release build"
	@echo "  publish    Publish to crates.io (dry-run first, 5s to abort)"
	@echo ""
	@echo "Variables:"
	@echo "  IFACE=$(IFACE)  PREFIX=$(PREFIX)  VERSION=$(VERSION)"
	@echo ""
	@echo "Examples:"
	@echo "  make run IFACE=vcan0"
	@echo "  make run IFACE=can0 EXTRA='-t delta -q'"
	@echo "  make install PREFIX=/usr/local"
