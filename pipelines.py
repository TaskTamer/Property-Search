# pipelines.py
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from scrapy.exceptions import DropItem

# Replace 'sqlite:///data.db' with your desired database connection string.
engine = create_engine('sqlite:///data.db')
Base = declarative_base()


class House(Base):
    __tablename__ = 'houses'
    id = Column(Integer, primary_key=True, autoincrement=True)
    price = Column(String)
    # location = Column(String)
    details_url = Column(String)
    bedrooms = Column(Integer)
    bathrooms = Column(Integer)
    area = Column(String)
    int_price = Column(Integer)
    latitude = Column(Float)
    longitude = Column(Float)
    phone = Column(String)
    contact_name = Column(String)
    # property_type = Column(String)


class Plot(Base):
    __tablename__ = 'plots'
    id = Column(Integer, primary_key=True, autoincrement=True)
    price = Column(String)
    # location = Column(String)
    details_url = Column(String)
    bedrooms = Column(Integer)
    bathrooms = Column(Integer)
    area = Column(String)
    int_price = Column(Integer)
    latitude = Column(Float)
    longitude = Column(Float)
    phone = Column(String)
    contact_name = Column(String)
    # property_type = Column(String)


class DataPipeline:
    def __init__(self):
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        self.session = Session()

    def process_item(self, item, spider):
        if item['property_type'] == 'house':
            try:
                house = House(
                    price=item['price'],
                    # location=item['location'],
                    details_url=item['details_url'],
                    bedrooms=item['bedrooms'],
                    bathrooms=item['bathrooms'],
                    area=item['area'].replace(',', ''),
                    int_price=int(item['int_price']),
                    latitude=round(float(item['latitude']), 5),
                    longitude=round(float(item['longitude']), 5),
                    phone=item['phone'],
                    contact_name=item['contact_name'],
                    # property_type=item['property_type'],
                )

                self.session.add(house)
                self.session.commit()

            except Exception as e:
                self.session.rollback()
                raise DropItem(f"Error processing item: {str(e)}")

        elif item['property_type'] == 'plot':
            try:
                plot = Plot(
                    price=item['price'],
                    # location=item['location'],
                    details_url=item['details_url'],
                    bedrooms=item['bedrooms'],
                    bathrooms=item['bathrooms'],
                    area=item['area'].replace(',', ''),
                    int_price=int(item['int_price']),
                    latitude=round(float(item['latitude']), 5),
                    longitude=round(float(item['longitude']), 5),
                    phone=item['phone'],
                    contact_name=item['contact_name'],
                    # property_type=item['property_type'],
                )

                self.session.add(plot)
                self.session.commit()

            except Exception as e:
                self.session.rollback()
                raise DropItem(f"Error processing item: {str(e)}")

        return item

    def close_spider(self, spider):
        # Commit all changes and close the session when the spider is closed

        self.session.close()
