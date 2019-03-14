class SQL:
    def __init__(self):
        self._fields = []
        self._table = ''
        self._sample = None
        self._criteria = []
        self._groupby = []
        self._orders = []
        self._union = []

    def table(self, table):
        self._table = table
        return self

    def select(self, fields):
        self._fields += [i.strip() for i in fields if i.strip()]
        return self

    def where(self, criteria):
        if criteria is not None and len(criteria) > 0:
            self._criteria += [i.strip() for i in criteria if i.strip()]
        return self

    def groupby(self, grp):
        self._groupby += [i.strip() for i in grp if i.strip()]
        return self

    def orderby(self, orders):
        self._orders += [i.strip() for i in orders if i.strip()]
        return self

    def union(self, others):
        self._union += others
        return self

    def sample(self, ratio):
        self._sample = ratio
        return self

    def _buildMe(self):
        s = []
        if self._fields:
            s.append('select {}'.format(', '.join(self._fields)))
        else:
            s.append('select *')

        if self._table:
            s.append('from {}'.format(self._table))
        else:
            raise ValueError('missing table name')

        if self._sample:
            s.append('sample {}'.format(self._sample))

        if self._criteria:
            date_criteria = list(filter(lambda x: x.startswith('EventDate'), self._criteria))
            if date_criteria:
                s.append('prewhere {}'.format(' and '.join(date_criteria)))
            rest_criteria = list(set(self._criteria) - set(date_criteria))
            s.append('where {}'.format(' and '.join(rest_criteria)))

        if self._groupby:
            s.append('group by {}'.format(', '.join(self._groupby)))

        if self._orders:
            s.append('order by {}'.format(', '.join(self._orders)))

        return ' '.join(s)

    def build_all(self):
        parts = [self._buildMe()]

        if self._union:
            parts += [o.build_all() for o in self._union]

        return '\nUNION ALL\n'.join(parts)

    def to_string(self):
        return self.build_all()