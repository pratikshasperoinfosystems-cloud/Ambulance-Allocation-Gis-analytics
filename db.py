from databases import Database
DATABASE_URL = (
    "postgresql+asyncpg://postgres:%24per0%40lZ3%232026%24@192.168.1.133:5432/ambulance_allocation"
)


database = Database(DATABASE_URL, min_size=1, max_size=10)

