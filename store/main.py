import asyncio
import json
from typing import Set, Dict, List, Any, Union
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Body

from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    Float,
    DateTime, update,
)
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.sql import select
from datetime import datetime
from pydantic import BaseModel, field_validator, Field, parse_obj_as, ConfigDict
from config import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)

# FastAPI app setup
app = FastAPI()
# SQLAlchemy setup
DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
engine = create_engine(DATABASE_URL)
metadata = MetaData()

""" processed_agent_data = Table(
    "processed_agent_data",
    metadata,
    Column("id", Integer, primary_key=True, index=True),
    Column("road_state", String),
    Column("user_id", Integer),
    Column("x", Float),
    Column("y", Float),
    Column("z", Float),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("timestamp", DateTime),
)"""

Base = declarative_base()


# Define SQLAlchemy ORM mapped class
class ProcessedAgentDataDBM(Base):
    __tablename__ = 'processed_agent_data'

    id = Column(Integer, primary_key=True, autoincrement=True)
    road_state = Column(String)
    user_id = Column(Integer)
    x = Column(Float)
    y = Column(Float)
    z = Column(Float)
    latitude = Column(Float)
    longitude = Column(Float)
    timestamp = Column(DateTime)


Base.metadata.create_all(engine)
SessionLocal = sessionmaker(bind=engine)


# SQLAlchemy model
class ProcessedAgentDataInDB(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    road_state: str
    user_id: int
    x: float
    y: float
    z: float
    latitude: float
    longitude: float
    timestamp: datetime


# FastAPI models
class AccelerometerData(BaseModel):
    x: float
    y: float
    z: float


class GpsData(BaseModel):
    latitude: float
    longitude: float


class AgentData(BaseModel):
    user_id: int
    accelerometer: AccelerometerData
    gps: GpsData
    timestamp: datetime

    @classmethod
    @field_validator("timestamp", mode="before")
    def check_timestamp(cls, value):
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(value)
        except (TypeError, ValueError):
            raise ValueError(
                "Invalid timestamp format. Expected ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ)."
            )


class ProcessedAgentData(BaseModel):
    road_state: str
    agent_data: AgentData


# WebSocket subscriptions
subscriptions: Dict[int, Set[WebSocket]] = {}


# FastAPI WebSocket endpoint
@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: int):
    await websocket.accept()
    if user_id not in subscriptions:
        subscriptions[user_id] = set()
    subscriptions[user_id].add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        subscriptions[user_id].remove(websocket)


# Function to send data to subscribed users
async def send_data_to_subscribers(user_id: int, data):
    if user_id in subscriptions:
        for websocket in subscriptions[user_id]:
            await websocket.send_json(json.dumps(data))


# FastAPI CRUDL endpoints


@app.post("/processed_agent_data/",)
async def create_processed_agent_data(data: List[ProcessedAgentData]):
    # Insert data to database
    # Send data to subscribers
    with SessionLocal() as ssl:
        temp_data = [
            ProcessedAgentDataDBM(
                road_state=item.road_state,
                user_id=item.agent_data.user_id,
                x=item.agent_data.accelerometer.x,
                y=item.agent_data.accelerometer.y,
                z=item.agent_data.accelerometer.z,
                latitude=item.agent_data.gps.latitude,
                longitude=item.agent_data.gps.longitude,
                timestamp=item.agent_data.timestamp,
            ) for item in data
            ]
        
        ssl.add_all(temp_data)
        ssl.commit()

        for sub_id in subscriptions:
            await send_data_to_subscribers(sub_id, temp_data)

        print("Success")


@app.get(
    "/processed_agent_data/{processed_agent_data_id}",
    response_model=Union[ProcessedAgentDataInDB, None],
)
def read_processed_agent_data(processed_agent_data_id: int):
    # Get data by id
    with SessionLocal() as ssl:
        get_rec = ssl.query(ProcessedAgentDataDBM).get(processed_agent_data_id)

        if get_rec is None:
            raise HTTPException(status_code=404, detail="Processed agent data not found")

        return ProcessedAgentDataInDB.model_validate(get_rec)


@app.get("/processed_agent_data/", response_model=list[ProcessedAgentDataInDB])
def list_processed_agent_data():
    # Get list of data
    with SessionLocal() as ssl:
        return ssl.query(ProcessedAgentDataDBM).all()

@app.put(
    "/processed_agent_data/{processed_agent_data_id}",
    response_model=ProcessedAgentDataInDB,
)
async def update_processed_agent_data(processed_agent_data_id: int, data: ProcessedAgentData):
    # Update data
    # Insert data to database
    # Send data to subscribers
    with SessionLocal() as ssl:
        upd_data = ssl.query(ProcessedAgentDataDBM).get(processed_agent_data_id)
        if upd_data is None:
            raise HTTPException(status_code=404, detail="Processed agent data not found")

        con_updated_data = ProcessedAgentDataDBM(
            road_state=data.road_state,
            user_id=data.agent_data.user_id,
            x=data.agent_data.accelerometer.x,
            y=data.agent_data.accelerometer.y,
            z=data.agent_data.accelerometer.z,
            latitude=data.agent_data.gps.latitude,
            longitude=data.agent_data.gps.longitude,
            timestamp=data.agent_data.timestamp,
        )

        upd_data.road_state = con_updated_data.road_state,
        upd_data.user_id = con_updated_data.user_id,
        upd_data.x = con_updated_data.x,
        upd_data.y = con_updated_data.y,
        upd_data.z = con_updated_data.z,
        upd_data.latitude = con_updated_data.latitude,
        upd_data.longitude = con_updated_data.longitude,
        upd_data.timestamp = con_updated_data.timestamp,

        ssl.commit()

        for sub_id in subscriptions:
            await send_data_to_subscribers(sub_id, upd_data)

        return ProcessedAgentDataInDB.model_validate(upd_data)


@app.delete(
    "/processed_agent_data/{processed_agent_data_id}",
    response_model=ProcessedAgentDataInDB,
)
def delete_processed_agent_data(processed_agent_data_id: int):
    # Delete by id
    with SessionLocal() as ssl:
        del_data = ssl.query(ProcessedAgentDataDBM).get(processed_agent_data_id)
        if del_data is None:
           raise HTTPException(status_code=404, detail="Processed agent data not found")

        ssl.query(ProcessedAgentDataDBM).filter(ProcessedAgentDataDBM.id == processed_agent_data_id).delete()
        ssl.commit()

        return ProcessedAgentDataInDB.model_validate(del_data)
    
    

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)