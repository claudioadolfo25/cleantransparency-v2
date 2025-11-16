#!/usr/bin/env bash
set -e

# Crear estructura
mkdir -p src/{api,workflows,signing,audit,services,agents,db,utils}
mkdir -p src/api/routes src/api/deps src/api/middleware
mkdir -p src/workflows/art17
mkdir -p infra scripts tests

echo "📁 Estructura creada"
