from sjasoft.uop.db_collection import DBCollection
from sjasoft.uop.database import Database
from sjasoft.uop.collections import meta_kinds
from sjasoft.uop.db_interface import Interface
from sjasoft.uop.query import Q
from sjasoft.uop.index import Index
from sjasoft.utils.dicts import first_kv, with_only, DictObject
import re   
from collections import defaultdict
from sjasoft.utils.data import recurse_set
from sjasoft.utils.iterext import take
from sjasoft.uopmeta.schemas.meta import Related
from sjasoft.uopmeta import oid

class MemCollection(DBCollection):
    def __init__(self, db, kind):   
        self._collection = db.raw_collections[kind]
        super().__init__(self._collection)
        
    
    def satisfying_query(self, query):
        func = Q.query_function(query)
        return (i for i in self._collection.values() if func(i))   
        
    def insert(self, **kwargs):
        an_id = kwargs.get('id')
        if not an_id:
            an_id = Index.make_id(48)
            kwargs['id'] = an_id
        self._collection[an_id] = kwargs
        return an_id    

    def update(self, query, update):
        base = self.satisfying_query(query)
        for i in base:
            yield i.update(update)

    def find(self, criteria=None, only_cols=None,
        order_by=None, limit=None, ids_only=False):
        only_cols = only_cols or []
        order_by = [o for o in order_by if o in only_cols] if order_by else []

        def _base_find():
            criteria = criteria or {}
            return self.satisfying_query(criteria)
    
        gen = _base_find()
        if ids_only:
            gen =  (i['id'] for i in gen)
        elif only_cols:
            gen =  ({k: i[k] for k in only_cols} for i in gen)
        if order_by:
            return sorted(_base_find(), key=lambda x: [getattr(x, i) for i in order_by])
        if limit:
            gen = take(limit, gen)
        return list(gen)

    def ids_only(self, criteria=None):
        return self.find(criteria, ids_only=True)

    def find_one(self, criteria=None, only_cols=None):
        return self.find(criteria, only_cols, limit=1)[0]

    def delete(self, query):
        if isinstance(self._collection, dict):
            ids = iter() if isinstance(query, str) else self.ids_only(query)
            for id in ids:
                self._collection.pop(id)
        elif isinstance(self._collection, set):
            to_delete = set(self.satisfying_query(query))
            self._collection -= to_delete


    def get(self, id):
        return self._collection.get(id)


class RelatedCollection(MemCollection):
    # fix using dicts here as set elments
    def __init__(self, collection: set):
        self._collection = collection
        self._cache = {}
        super().__init__(collection)

    def satisfying_query(self, query):
        func = Q.query_function(query)
        return (i.dict() for i in self._collection if func(i.dict()))         

    def _rel_dict_set(self, an_id, id_field, key_field, other_field, reverse=False):
        res = defaultdict(set)
        for i in self.satisfying_query(dict(id_field=an_id)):
            if reverse:
                res[i[other_field]].add(i[key_field])
            else:
                res[i[key_field]].add(i[other_field])
        return res

    def get_roleset(self, id, role_id, reverse=False):
        if reverse:
            return set(i['subject_id'] for i in self._collection if i['object_id'] == id and i['assoc_id'] == role_id)
        return set(i['object_id'] for i in self._collection if i['subject_id'] == id and i['assoc_id'] == role_id)

    def get_all_related_by(self, role_id, reverse=False):
        return self._rel_dict_set(role_id, 'assoc_id', 'subject_id', 'object_id', reverse)

    def get_all_related(self, id):
        return set(i['subject_id'] for i in self._collection if i['object_id'] == id) | \
            set(i['object_id'] for i in self._collection if i['subject_id'] == id)

    def get_related_role_map(self, id, reverse=False):
        res = defaultdict(set)
        for i in self._collection:
            if reverse:
                res[i['assoc_id']].add(i['subject_id'])
            else:
                res[i['assoc_id']].add(i['object_id'])
        return res

    def insert(self, **kwargs):
        object = Related(**kwargs)
        self._collection.add(object)
        return object.dict()
    
    def delete(self, query):
        super().delete(query)
        
        
    def get(self, id):
        return next((i for i in self._collection if i['id'] == id), None)
    
    def drop(self):
        self._collection.clear()

    def update(self, query, update):
        base = self.satisfying_query(query)
        for i in base:
            i.update(update)

class MemDB(Interface):
    def __init__(self, on_disk=''):
        self._on_disk = on_disk
        self._mem_collections = dict(
            classes = {},
            attributes = {},
            roles = {},
            tags = {},
            groups = {},
            queries = {},
            related = set(),
            changes = {},
            schemas = {},
            class_instances = defaultdict(dict)
        )
        self._collections = DictObject(
            classes = MemCollection(self,'classes'),
            attributes = MemCollection(self._mem_collections['attributes']),
            roles = MemCollection(self._mem_collections['roles']),
            tags = MemCollection(self._mem_collections['tags']),
            groups = MemCollection(self._mem_collections['groups']),
            queries = MemCollection(self._mem_collections['queries']),
            related = RelatedCollection(self._mem_collections['related']),
            changes = MemCollection(self._mem_collections['changes']),
            schemas = MemCollection(self._mem_collections['schemas']),
        )
        pass


    @property
    def raw_db(self):
        return self

    def get_metadata(self):
        return {k:self.raw_colletions[k].values() for k in meta_kinds}

    @property
    def raw_collections(self):
        return self._mem_collections

    @property
    def class_instances(self, class_id):
        return self._mem_collections['class_instances'][class_id]

    def drop_class_instances(self, class_id):
        self._mem_collections['class_instances'].pop(class_id)

    def get_collection(self, name):
        coll = self._collections.get(name)
        if not coll:
            self._mem_collections[name] = {}
            coll = self._collections[name] = MemCollection(self._mem_collections[name])
        return coll

    def get_object(self, uuid):
        cls_id = oid.oid_class(uuid)
        instances = self.class_instances(cls_id)
        return instances.get(uuid)    

    def drop_database(self):
        pass

    def list_collection_names(self):
        pass

    def get_tagset(self, tag_id):
        return self.related.get_roleset(tag_id, self.roles.by_name['tag_applies'])

    def get_object_tags(self, object_id):
        return self.related.get_roleset(object_id, self.roles.by_name['tag_applies'], reverse=True)

    def groups_in_group(self, group_id, recursive=False):
        fun = lambda x: self.related.get_roleset(x, self.roles.by_name['contains_group'])
        return recurse_set(fun(group_id), fun) if recursive else fun(group_id)

    def groups_containing_group(self, group_id, recursive=False ):
        fun = lambda x: self.related.get_roleset(x, self.roles.by_name['contains_group'], reverse=True)
        return recurse_set(fun(group_id), fun) if recursive else fun(group_id)

    def get_groupset(self, group_id):
        return self.related.get_roleset(group_id, self.roles.by_name['group_contains'])

    def get_object_groups(self, object_id):
        return self.related.get_roleset(object_id, self.roles.by_name['group_contains'], reverse=True)


    def get_roleset(self, subject, role_id, reverse=False):
        return self.related.get_roleset(subject, role_id, reverse)

    def get_all_related_by(self, role_id):
        return self.related.get_all_related_by(role_id)

    def get_all_related(self, id):
        return self.related.get_all_related(id)

    def get_related_role_map(self, id):
        return self.related.get_related_role_map(id)

    def relate(self, subject, role_id, object_id):
        self.related.insert(subject_id=subject, assoc_id=role_id, object_id=object_id)

    def unrelate(self, subject, role_id, object_id):
        self.related.delete(dict(subject_id=subject, assoc_id=role_id, object_id=object_id))

    def tag_neighbors(self, object_id):
        tag_ids = self.get_object_tags(object_id)
        return by_name

    
