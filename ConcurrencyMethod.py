class ConcurrencyMethod:
    def __init__(self):
        pass
    
    def log_object(self, object, transaction_id: int) -> None:
        """Mencatat objek yang diakses oleh transaksi."""
        pass
    
    def validate_object(self, object, transaction_id: int, action) -> 'Response':
        """Memvalidasi apakah transaksi boleh melakukan aksi tertentu pada objek."""
        pass
    
    def end_transaction(self, transaction_id: int) -> None:
        """Mengakhiri transaksi."""
        pass