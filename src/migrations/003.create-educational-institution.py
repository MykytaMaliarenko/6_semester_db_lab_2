from yoyo import step

__depends__ = ['002.create-place']


steps = [
    step(
        """
        CREATE TABLE educational_institution (
            id serial primary key, 
            name varchar(500) not null,
            type varchar(500),
            place_id integer not null, 
            parent_body_name varchar(500),
            CONSTRAINT fk_place_id
              FOREIGN KEY(place_id) 
              REFERENCES place(id)
              ON DELETE RESTRICT
        )
        """,
        "DROP TABLE educational_institution",
    ),
]
