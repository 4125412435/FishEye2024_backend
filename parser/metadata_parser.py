class Metadata:
    def __init__(self, last_edited_by, last_edited_date, date_added, raw_source, algorithm):
        self.last_edited_by = last_edited_by
        self.last_edited_date = last_edited_date
        self.date_added = date_added
        self.raw_source = raw_source
        self.algorithm = algorithm


def parse_metadata(json_node):
    last_edited_by = json_node['_last_edited_by']
    last_edited_date = json_node['_last_edited_date']
    date_added = json_node['_date_added']
    raw_source = json_node['_raw_source']
    algorithm = json_node['_algorithm']
    return Metadata(last_edited_by, last_edited_date, date_added, raw_source, algorithm)
