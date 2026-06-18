from pyiceberg.catalog.sql import SqlCatalog
import os

def get_catalog():
    """
    Catalog = Iceberg ka directory/registry
    Kaunsi tables hain, kahan stored hain — sab catalog track karta hai
    
    SqlCatalog = SQLite mein metadata store (dev ke liye)
    Production mein: AWS Glue Catalog, Hive Metastore, Nessie
    """
    os.makedirs("iceberg/warehouse", exist_ok=True)
    
    catalog = SqlCatalog(
        "cricketpulse",              # catalog naam
        **{
            "uri": "sqlite:///iceberg/iceberg_catalog.db",
            # metadata kahan store ho
            
            "warehouse": "iceberg/warehouse",
            # actual data files kahan jaayein
        }
    )
    return catalog

def setup_namespace(catalog):
    """
    Namespace = database/schema jaise concept
    Tables organize karne ke liye
    """
    try:
        catalog.create_namespace("cricket")
        print(" Namespace 'cricket' created")
    except Exception:
        print(" Namespace 'cricket' already exists")

if __name__ == "__main__":
    catalog = get_catalog()
    setup_namespace(catalog)
    
    # Existing namespaces dekho
    print(f"\nNamespaces: {catalog.list_namespaces()}")