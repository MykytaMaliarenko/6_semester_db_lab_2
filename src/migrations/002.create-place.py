import typing as t
from yoyo import step

__depends__ = ['001.create-zno-data']


def generate_locality_table(
    table_name: str,
    extra: t.Optional[str] = None
) -> str:
    return f"""
        create table {table_name} (
            id serial primary key, 
            name varchar(250) unique NOT NULL {',' if extra else ''}
            {extra or ''}
        )
    """


steps = [
    step(
        generate_locality_table('region'),
        "DROP TABLE region",
    ),
    step("insert into region (name) select distinct regname from zno_data"),
    step(
        generate_locality_table('area'),
        "DROP TABLE area",
    ),
    step(
        """
        insert into area (name) select distinct areaname 
        from zno_data on conflict do nothing
        """
    ),
    step(
        """
        insert into area (name) 
        (select distinct eoareaname from zno_data where eoareaname is not null)
        on conflict do nothing 
        """
    ),
    step(
        generate_locality_table('territory', 'type varchar(100)'),
        "DROP TABLE territory",
    ),
    step(
        """
        insert into territory (name, type) 
        select distinct tername, tertypename from zno_data 
        on conflict do nothing
        """
    ),
    step(
        """
        insert into territory (name) 
        (select distinct eotername from zno_data where eotername is not null)
        on conflict do nothing 
        """
    ),
    step(
        """
        CREATE TABLE place (
            id serial primary key, 
            region_id integer not null, 
            area_id integer not null,
            territory_id integer not null,
            CONSTRAINT fk_region
              FOREIGN KEY(region_id) 
              REFERENCES region(id)
              ON DELETE RESTRICT,
            CONSTRAINT fk_area
              FOREIGN KEY(area_id) 
              REFERENCES area(id)
              ON DELETE RESTRICT,
            CONSTRAINT fk_territory
              FOREIGN KEY(territory_id) 
              REFERENCES territory(id)
              ON DELETE RESTRICT
        )
        """,
        "DROP TABLE territory",
    ),
]
