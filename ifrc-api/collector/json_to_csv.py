import csv
from .common import gen_output_path


class ToCsv():
    def __init__(self, headers=[], key_separator='__', fix_headers=False):
        self.key_separator = key_separator
        self.fix_headers = fix_headers
        self.csv_data = []
        self.headers = {}
        self._header_counter = 1
        for header in headers:
            self._add_header(header, True)

    def _add_header(self, header, force=False):
        if self.headers.get(header) is None and\
                (not self.fix_headers or force):
            self.headers[header] = self._header_counter
            self._header_counter += 1

    def _extract_value(self, value, pkey, output):
        if type(value) is dict:
            for key in value.keys():
                f_key = self.key_separator.join([pkey, key])
                self._extract_value(value[key], f_key, output)
        elif type(value) is list:
            for i, item in enumerate(value):
                index = item.get('source', i)
                f_key = self.key_separator.join([pkey, str(index)])
                self._extract_value(item, f_key, output)
        else:
            self._add_header(pkey)
            output[pkey] = value

    def _extract_datum(self, datum):
        out = {}
        for key in datum.keys():
            self._extract_value(datum[key], key, out)
        return out

    def _flatten_dict(self, data):
        for datum in data:
            self.csv_data.append(
               self._extract_datum(datum)
            )
        return self

    def _get_headers(self):
        headers = sorted(self.headers, key=self.headers.get)
        return headers

    def save_csv(self, path, data):
        """
        Extract and save dict to CSV file
        """
        self._flatten_dict(data)
        headers = self._get_headers()
        with open(path, mode='w') as file:
            writer = csv.DictWriter(
                file, fieldnames=headers, extrasaction='ignore',
            )
            writer.writeheader()
            for data in self.csv_data:
                writer.writerow(data)
        return self


if __name__ == '__main__':
    data = [{
        'key1': 'NP',
        'key2': 'Togglecorp',
        'key3': {
            'key3a': 'hello3a',
            'key3b': 'hello3b',
        },
        'key4': 'Togglecorp',
        'key5': {
            'key5a': {
                'key5aa': 'hello5aa',
            },
            'key3b': 'hello',
        },
    }, {
        'key1': 'NP',
        'key2': 'Togglecorp',
        'key4': 'Togglecorp for key4',
    }]

    headers = ['key2', 'key1']

    ToCsv(headers, key_separator='_').save_csv(
        gen_output_path('_test.csv'), data
    )
