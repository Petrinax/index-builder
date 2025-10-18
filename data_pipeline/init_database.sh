#!/bin/bash

# Database Initialization Script
# This script initializes the database schema from DDL scripts

# Default values
DB_TYPE="duckdb"
DB_PATH=""
VERIFY_ONLY=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --db-type)
            DB_TYPE="$2"
            shift 2
            ;;
        --db-path)
            DB_PATH="$2"
            shift 2
            ;;
        --verify-only)
            VERIFY_ONLY=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --db-type <type>     Database type: duckdb or sqlite (default: duckdb)"
            echo "  --db-path <path>     Path to database file (optional)"
            echo "  --verify-only        Only verify schema without initializing"
            echo "  -h, --help          Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use -h or --help for usage information"
            exit 1
            ;;
    esac
done

# Build command
CMD="python3 init_database_pipeline.py --db-type $DB_TYPE"

if [ ! -z "$DB_PATH" ]; then
    CMD="$CMD --db-path $DB_PATH"
fi

if [ "$VERIFY_ONLY" = true ]; then
    CMD="$CMD --verify-only"
fi

# Execute
echo "Running database initialization pipeline..."
echo "Command: $CMD"
echo ""
eval $CMD

