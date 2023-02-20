from fastapi import FastAPI, status, File, Form, UploadFile, Response
import databases
import sqlalchemy
from sqlalchemy import Table, MetaData
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from pydantic import BaseModel
import os
import urllib
import geopy.distance
from predict.predictor import *



# class InData(BaseModel):
#     latlng: str
#     origimg: bytes

# class OutData(BaseModel):
#     predname: str
#     predconf : float
#     infTime : str
#     maskimg: bytes
   





host_server = os.environ.get('host_server', 'monumentrecog-db.postgres.database.azure.com')
db_server_port = urllib.parse.quote_plus(str(os.environ.get('db_server_port', '5432')))
database_name = os.environ.get('database_name', 'monument-recogdb')
db_username = urllib.parse.quote_plus(str(os.environ.get('db_username', 'monumentrecog')))
db_password = urllib.parse.quote_plus(str(os.environ.get('db_password', 'mlRecog$1')))
ssl_mode = urllib.parse.quote_plus(str(os.environ.get('ssl_mode','prefer')))
DATABASE_URL = 'postgresql://{}:{}@{}:{}/{}?sslmode={}'.format(db_username, db_password, host_server, db_server_port, database_name, ssl_mode)

database = databases.Database(DATABASE_URL)

def strtopoint(initcoord):
    initcoord = initcoord.replace(' ','')
    initcoord = initcoord[1:]
    initcoord = initcoord[:-1]
    initcoord = initcoord.split(',')
    intopoint = geopy.Point(float(initcoord[0]), float(initcoord[1]))
    return intopoint


def calculate_distance(initcoord):
    validclass = []
    for i, itrcoord in enumerate(coordlist):
        if(geopy.distance.geodesic(initcoord, itrcoord).km * 1000 < 200):
            validclass.append(((coords[i])[0]))
    return validclass







engine = sqlalchemy.create_engine(
    #DATABASE_URL, connect_args={"check_same_thread": False}
    DATABASE_URL, pool_size=3, max_overflow=0
)

metadata = sqlalchemy.MetaData(bind= engine)
sqlalchemy.MetaData.reflect(metadata)

inData = metadata.tables['receiveddata']
monumentData = metadata.tables['monument-data']

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)
app.add_middleware(GZipMiddleware)

@app.on_event("startup")
async def startup():
    await database.connect()
    query = sqlalchemy.select([
    monumentData.c.classindex,
    monumentData.c.latlng
    ])
    global coordlist, coords
    coordlist = []
    coords = engine.execute(query).fetchall()
    for row in coords:
        coordlist.append(strtopoint(row[1]))
    
 
    init()


@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()



async def hitDb(tablename, ltlg, origimg):
    query = tablename.insert().values(latlng=ltlg, origimg= origimg)
    last_record_id = await database.execute(query)



@app.post("/predict",  response_class=Response, status_code = status.HTTP_201_CREATED)
async def create_predict_data(  ltlg : str = Form(),file: UploadFile = File()):
    readfile = file.file.read()
    classlist = calculate_distance(initcoord= strtopoint(ltlg))
    result = run(readfile, classlist)
    await hitDb(inData, ltlg = ltlg, origimg = readfile)

    if result != 0 :
        classes, confd, inftime, maskbytes = result
        return Response( headers = { "predname" : classes, "predconf": str(confd),
         "infTime": inftime }, content = maskbytes, media_type="image/png")
    else:
        return{
            "predname" : '',
            "predconf" : 0.0,
            "infTime" : '',
            "maskimg" : ''

        }



@app.post("/predictbytes",  response_class=Response, status_code = status.HTTP_201_CREATED)
async def create_predict_data(  ltlg : str = Form(),file: bytes = File()):

    classlist = calculate_distance(initcoord=ltlg)
    result = run(file, classlist)
    await hitDb(inData, ltlg = ltlg, origimg = file)
  
    
    if result != 0 :
        classes, confd, inftime, maskbytes = result
        return Response( headers = { "predname" : classes, "predconf": str(confd),
         "infTime": inftime }, content = maskbytes, media_type="image/png")
    else:
        return{
            "predname" : '',
            "predconf" : 0.0,
            "infTime" : '',
            "maskimg" : ''

        }
    






      
    
    
