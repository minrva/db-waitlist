# pylint: disable=invalid-name
"""Digests mod-waitlist json data."""
import json
import os
import uuid
import dataset


# directory
DATA_DIR = 'data'
COURSES_FILE = 'courses.json'
INSTRS_FILE = 'instructors.json'

# postgres
PG_USER = 'folio_admin'
PG_PASSWORD = 'folio_admin'
PG_NETLOC = '10.0.2.15'
PG_PORT = '5432'
PG_DBNAME = 'okapi_modules'
PG_URL = ("postgresql://" + PG_USER + ":" + PG_PASSWORD +
          '@' + PG_NETLOC + ':' + PG_PORT + '/' + PG_DBNAME)

INV_STORAGE_SCHEMA = 'diku_inventory_storage'
INV_ITEM_TBL = 'item'

WL_SCHEMA = 'diku_mod_waitlist'
WL_COURSES_TBL = 'courses'
WL_INSTRUCTORS_TBL = 'instructors'
WL_ITEMS_TBL = 'items'


def load_json(fpath):
    """Loads a JSON file."""
    with open(fpath) as fs:
        d = json.load(fs)
    print("Loaded {0} objects from {1}...".format(len(d), fpath))
    return d


def load_table(tbl_name, schema_name):
    """Loads a postgres table."""
    rows = []
    with dataset.Database(url=PG_URL, schema=schema_name) as db:
        tbl = db[tbl_name]
        print("Loaded {0} rows from {1}.{2}...".format(
            len(tbl), schema_name, tbl_name))
        for row in tbl:
            rows.append(row)
    db.executable.close()
    db = None
    return rows


def create_table(rows, tbl_name, schema_name, clear=True):
    """Creates a postgres table."""
    print("Saving {0} rows to {1}.{2}...".format(
        len(rows), schema_name, tbl_name))
    with dataset.Database(url=PG_URL, schema=schema_name) as db:
        table = db[tbl_name]
        if clear:
            table.delete()
        table.insert_many(rows)
    db.executable.close()
    db = None


def create_course_row(data):
    """Creates a course row."""
    new_obj = {}
    new_obj['id'] = str(uuid.uuid4())
    new_obj['name'] = data['name']
    return dict(jsonb=new_obj)


def create_instructor_row(data):
    """Creates an instructor row."""
    new_obj = {}
    new_obj['id'] = str(uuid.uuid4())
    new_obj['name'] = data['name']
    return dict(jsonb=new_obj)


def create_reserve_row(data, instr_data, course_data):
    """Creates a reserve from an item, instructor, and course."""
    new_obj = {}
    new_obj['id'] = str(uuid.uuid4())
    new_obj['title'] = data['jsonb']['title']
    new_obj['location'] = data['jsonb']['location']['name']
    new_obj['barcode'] = data['jsonb']['barcode']
    new_obj['instructorId'] = instr_data['jsonb']['id']
    new_obj['instructor'] = instr_data['jsonb']['name']
    new_obj['courseId'] = course_data['jsonb']['id']
    new_obj['course'] = course_data['jsonb']['name']
    return dict(jsonb=new_obj)


if __name__ == '__main__':

    # load courses, instructors, and items data
    print('Loading data...')
    courses_path = os.path.join(DATA_DIR, COURSES_FILE)
    courses_json = load_json(courses_path)
    courses_json = courses_json[1:len(courses_json)]
    instrs_path = os.path.join(DATA_DIR, INSTRS_FILE)
    instrs_json = load_json(instrs_path)
    instrs_json = instrs_json[1:len(instrs_json)]
    items_json = load_table(INV_ITEM_TBL, INV_STORAGE_SCHEMA)

    # normalize list lengths
    print('Normalizing data...')
    norm_len = min(len(courses_json), len(instrs_json), len(items_json))
    courses_json = courses_json[0:norm_len]
    instrs_json = instrs_json[0:norm_len]
    items_json = items_json[0:norm_len]

    # transform data to database rows
    print('Transforming data to database rows...')
    course_rows = []
    instr_rows = []
    item_rows = []
    for course_json, instr_json, item_json, in zip(courses_json, instrs_json, items_json):
        course_row = create_course_row(course_json)
        instr_row = create_instructor_row(instr_json)
        item_row = create_reserve_row(item_json, instr_row, course_row)
        course_rows.append(course_row)
        instr_rows.append(instr_row)
        item_rows.append(item_row)

    # create database tables
    print('Creating database tables...')
    create_table(course_rows, WL_COURSES_TBL, WL_SCHEMA)
    create_table(instr_rows, WL_INSTRUCTORS_TBL, WL_SCHEMA)
    create_table(item_rows, WL_ITEMS_TBL, WL_SCHEMA)

    print('Complete...')
