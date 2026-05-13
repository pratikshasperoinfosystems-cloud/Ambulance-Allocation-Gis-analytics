from fastapi import FastAPI
from db import database

app = FastAPI()


@app.on_event("startup")
async def startup():
    await database.connect()


@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()
###############################################Ambulance API#######################################################################
@app.get("/ambulances")
async def get_ambulances():

    query = """
        SELECT *
        FROM (
            SELECT 
                ea.amb_rto_register_no,
                ea.amb_lat,
                ea.amb_log,

                md.dst_name AS district_name,
                dv.div_name AS division_name,

                amt.ambt_name AS ambulance_status,

                ROW_NUMBER() OVER (
                    PARTITION BY ea.amb_rto_register_no
                    ORDER BY ea.amb_rto_register_no
                ) AS rn

            FROM ems_ambulance ea

            LEFT JOIN ems_mas_districts md
                ON ea.amb_district = md.dst_code

            LEFT JOIN ems_mas_division dv
                ON ea.amb_div_code = dv.div_code

            LEFT JOIN ems_mas_ambulance_type amt
                ON TRIM(ea.amb_type::text) = TRIM(amt.ambu_level::text)

            WHERE ea.ambis_deleted = '0'
        ) t
        WHERE rn = 1
    """

    rows = await database.fetch_all(query)

    ambulance_list = []

    for row in rows:
        ambulance_list.append({
            "ambulance_no": row["amb_rto_register_no"],
            "latitude": row["amb_lat"],
            "longitude": row["amb_log"],
            "district": row["district_name"],
            "division": row["division_name"],
            "ambulance_status": row["ambulance_status"]
        })

    return {
        "ambulances": ambulance_list
    }
#############################################################################################################