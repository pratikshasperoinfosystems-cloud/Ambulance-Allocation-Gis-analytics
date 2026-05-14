from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from db import database
from cachetools import TTLCache
import asyncio
import time
from math import radians, sin, cos, sqrt, atan2
import json
import re
import numpy as np
from scipy.spatial import KDTree
from math import radians
import re
from functools import lru_cache



app = FastAPI()


@app.on_event("startup")
async def startup():
    await database.connect()


@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()
####################################################Cache function##############################################
_cache = {}
_cache_expiry = {}

async def cached_query(sql, params=None, ttl=15, fetch="all", db=database):
    """
    Run cached query with support for multiple databases.
    
    Args:
        sql (str): SQL query
        params (tuple/dict): query params
        ttl (int): cache expiry in seconds
        fetch (str): "all" or "one"
        db (Database): which database object to use (default = database)
    """
    key = (id(db), sql, str(params), fetch)  
    now = time.time()

    if key in _cache and now < _cache_expiry[key]:
        return _cache[key]

    if fetch == "one":
        result = await db.fetch_one(sql, params)
    else:
        result = await db.fetch_all(sql, params)

    _cache[key] = result
    _cache_expiry[key] = now + ttl
    return result

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

    rows = await cached_query(
        query,
        ttl=30
    )

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

######################################Ambulance Counts Websocket#######################################################################
@app.websocket("/ws/ambulance_counts")
async def ambulance_counts_district_ws(websocket: WebSocket):

    await websocket.accept()

    prev_data = {}

    try:

        while True:

            # =========================
            # FETCH AMBULANCE TYPES
            # =========================
            type_query = """
                SELECT
                    ambt_id,
                    ambt_name
                FROM ems_mas_ambulance_type
                WHERE ambtis_deleted = '0'
                ORDER BY ambt_id
            """

            type_result = await cached_query(
                type_query,
                ttl=300
            )

            amb_type_mapping = {

                str(row["ambt_id"]): row["ambt_name"]

                for row in type_result
            }

            # =========================
            # MAIN QUERY
            # =========================
            query = """
                SELECT
                    md.dst_name,

                    ea.amb_type,

                    SUM(
                        CASE
                            WHEN ea.amb_status = '7'
                            THEN 1
                            ELSE 0
                        END
                    ) AS offroad_count,

                    SUM(
                        CASE
                            WHEN ea.amb_status != '7'
                            AND ea.amb_status != '5'
                            THEN 1
                            ELSE 0
                        END
                    ) AS onroad_count,

                    SUM(
                        CASE
                            WHEN ea.amb_status = '1'
                            THEN 1
                            ELSE 0
                        END
                    ) AS free_count,

                    SUM(
                        CASE
                            WHEN ea.amb_status = '2'
                            THEN 1
                            ELSE 0
                        END
                    ) AS busy_count,

                    COUNT(*) AS total_count

                FROM ems_ambulance ea

                LEFT JOIN ems_mas_districts md
                    ON ea.amb_district = md.dst_code

                WHERE ea.ambis_deleted = '0'

                GROUP BY
                    md.dst_name,
                    ea.amb_type
            """

            result = await cached_query(
                query,
                ttl=10
            )

            # =========================
            # RESPONSE STRUCTURE
            # =========================
            overall_data = {}

            district_data = {}

            total_all = 0
            total_onroad_all = 0
            total_offroad_all = 0
            total_free_all = 0
            total_busy_all = 0

            # =========================
            # PROCESS DATA
            # =========================
            for row in result:

                district_name = row["dst_name"] or "UNKNOWN"

                amb_type_name = amb_type_mapping.get(
                    str(row["amb_type"]),
                    f"TYPE_{row['amb_type']}"
                )

                offroad = int(row["offroad_count"] or 0)

                onroad = int(row["onroad_count"] or 0)

                free = int(row["free_count"] or 0)

                busy = int(row["busy_count"] or 0)

                total = int(row["total_count"] or 0)

                # OVERALL
                if amb_type_name not in overall_data:

                    overall_data[amb_type_name] = {

                        "total": 0,

                        "onroad": 0,

                        "offroad": 0,

                        "free": 0,

                        "busy": 0
                    }

                overall_data[amb_type_name]["total"] += total
                overall_data[amb_type_name]["onroad"] += onroad
                overall_data[amb_type_name]["offroad"] += offroad
                overall_data[amb_type_name]["free"] += free
                overall_data[amb_type_name]["busy"] += busy

                # GRAND TOTAL
                total_all += total
                total_onroad_all += onroad
                total_offroad_all += offroad
                total_free_all += free
                total_busy_all += busy

                # DISTRICT
                if district_name not in district_data:

                    district_data[district_name] = {}

                if amb_type_name not in district_data[district_name]:

                    district_data[district_name][amb_type_name] = {

                        "total": 0,

                        "onroad": 0,

                        "offroad": 0,

                        "free": 0,

                        "busy": 0
                    }

                district_data[district_name][amb_type_name]["total"] += total

                district_data[district_name][amb_type_name]["onroad"] += onroad

                district_data[district_name][amb_type_name]["offroad"] += offroad

                district_data[district_name][amb_type_name]["free"] += free

                district_data[district_name][amb_type_name]["busy"] += busy

            # =========================
            # ADD MISSING TYPES
            # =========================
            for type_name in amb_type_mapping.values():

                if type_name not in overall_data:

                    overall_data[type_name] = {

                        "total": 0,

                        "onroad": 0,

                        "offroad": 0,

                        "free": 0,

                        "busy": 0
                    }

                for district in district_data:

                    if type_name not in district_data[district]:

                        district_data[district][type_name] = {

                            "total": 0,

                            "onroad": 0,

                            "offroad": 0,

                            "free": 0,

                            "busy": 0
                        }

            # =========================
            # TOTAL ROW
            # =========================
            overall_data["TOTAL"] = {

                "total": total_all,

                "onroad": total_onroad_all,

                "offroad": total_offroad_all,

                "free": total_free_all,

                "busy": total_busy_all
            }

            # =========================
            # FINAL RESPONSE
            # =========================
            final_response = {

                "overall": overall_data,

                "districts": district_data
            }

            # SEND ONLY IF CHANGED
            if final_response != prev_data:

                await websocket.send_json(final_response)

                prev_data = final_response

            await asyncio.sleep(15)

    except WebSocketDisconnect:

        print("WebSocket disconnected")

    except Exception as e:

        print(f"WebSocket Error: {e}")

        await websocket.close()
##########################################Villages Over 20 min api#############################################################
_village_centroid_cache = {}

def get_village_centroid(uid, geometry):
    if uid in _village_centroid_cache:
        return _village_centroid_cache[uid]

    coords = re.findall(
        r'\[\s*([0-9\.\-]+)\s*,\s*([0-9\.\-]+)\s*\]',
        geometry
    )
    if not coords:
        return None

    sample = coords[:20]
    lons = [float(c[0]) for c in sample]
    lats = [float(c[1]) for c in sample]
    centroid = (sum(lats) / len(lats), sum(lons) / len(lons))

    _village_centroid_cache[uid] = centroid
    return centroid


def build_ambulance_kdtree(ambulance_rows):
    points = []
    for amb in ambulance_rows:
        try:
            points.append([float(amb["amb_lat"]), float(amb["amb_log"])])
        except:
            continue

    if not points:
        return None, None

    arr = np.array(points)
    tree = KDTree(arr)
    return tree, arr
@app.get("/villages_over_20min")
async def villages_over_20min():

    ambulance_query = """
        SELECT amb_lat, amb_log FROM ems_ambulance
        WHERE ambis_deleted = '0'
          AND amb_lat IS NOT NULL AND amb_log IS NOT NULL
          AND amb_lat <> '' AND amb_log <> ''
    """
    village_query = """
        SELECT state, district, tehsil, village, uid, geometry
        FROM maha_village_v1
        WHERE geometry IS NOT NULL
    """

    ambulance_rows, village_rows = await asyncio.gather(
        cached_query(ambulance_query, ttl=300),
        cached_query(village_query, ttl=300),
    )

    tree, amb_arr = build_ambulance_kdtree(ambulance_rows)

    if tree is None:
        return {"total_villages": 0, "villages": []}

    village_data = []
    for v in village_rows:
        try:
            centroid = get_village_centroid(v["uid"], v["geometry"])
            if centroid:
                village_data.append((centroid, v))
        except:
            continue

    if not village_data:
        return {"total_villages": 0, "villages": []}

    
    centroids = np.array([d[0] for d in village_data])  # shape: (N, 2)

    distances_deg, _ = tree.query(centroids)

    distances_km = distances_deg * 111.0

    eta_minutes = (distances_km / 20) * 60
    village_list = [
        {
            "state": v["state"],
            "district": v["district"],
            "tehsil": v["tehsil"],
            "village": v["village"],
            "uid": v["uid"],
            "geometry": v["geometry"],
        }
        for (_, v), eta in zip(village_data, eta_minutes)
        if eta > 20
    ]

    return {
        "total_villages": len(village_list),
        "villages": village_list
    }