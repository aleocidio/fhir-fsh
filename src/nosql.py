#%%
from tinydb import TinyDB, Query
from tinydb import where
db = TinyDB('local_data.json')
downloads = db.table('downloads')
scrap = db.table('scrap')
recursos = db.table('recursos')
master_record = db.table('master_record')

Downloads = Query()
Scrap = Query()
Recursos = Query()
MasterRecord = Query()

"""
Downloads
    {
        'timestamp': "2022-06-04 17:24:58",
        'project': "RNDS",
        'files':[
            {fs_filename: "",
            meta_resourceType: "",
            meta_lastUpdated: "",
            meta_id: "",
            meta_name: "",
            meta_title: "",
            meta_url: "",
            meta_version: "",
            meta_status: "",
            meta_publisher: "",
            meta_type: "",
            meta_referencias: "",
            meta_reference_error: "",
            url_resourceType: "",
            url_name: "",
            sha256: "",
            uuid: ""}
        ]
    }
    
Scrap
{
    'timestamp': "2022-06-04 17:24:58",
    'project': "RNDS",
    'resources':[
        NAME: "",
        LINK: "",
        CANONICAL: "",	
        FILE: "",
        TYPE: "",
        TAG: "",
        DATETIME: ""   
    ]
}

Recursos
{
    'uuid': "",
    'created': "",
    'updated': "",
    'version': 'original',
    'json': ""
}

MasterRecord
    'project':"",
    'name':"",
    'canonical':"",
    'filename':"",
    'type':"",
    'tag':""
"""

#%%
db.insert({'name': 'John Doe', 'age': 42})
print(db.search(User.name == 'John Doe'))

#%%
db.all()[0]
# %%
db.update({'age': 31}, User.name == 'John Doe')
# %%
db.truncate()
# %%
db.all()
# %%
from unidecode import unidecode
db.search(User.name.map(unidecode) == 'Jose')
# will match 'José' etc.

#%%
db.search(where('field') == 'value')

#%%
db.search(User.name.exists())

#%%
db.search(User.name.matches('[aZ]*'))
import re
db.search(User.name.matches('John', flags=re.IGNORECASE))
db.search(User.name.search('b+'))

#%%
test_func = lambda s: s == 'John'
db.search(User.name.test(test_func))

#%%
def test_func(val, m, n):
    return m <= val <= n
db.search(User.age.test(test_func, 0, 21))
db.search(User.age.test(test_func, 21, 99))

#%%
# busca fragmento
db.search(Query().fragment({'foo': True, 'bar': False}))
# busca pelo valor do campo em um fragmento
db.search(Query().field.fragment({'foo': True, 'bar': False}))

#%%
db.insert({'name': 'user1', 'groups': ['user']})
db.insert({'name': 'user2', 'groups': ['admin', 'user']})
db.insert({'name': 'user3', 'groups': ['sudo', 'user']})

# %%
db.search(User.groups.any(['admin', 'sudo']))
# %%
groups.insert(
    {'name': 'user', 
     'permissions': [{'type': 'read'}]}
)
groups.insert(
    {'name': 'sudo', 
     'permissions': [{'type': 'sudo'}]}
)
groups.insert(
    {'name': 'admin', 
     'permissions': [{'type': 'read'}, {'type': 'write'}, {'type': 'sudo'}]}
)
# %%
#grupos com permissão type = read
groups.search(Group.permissions.any(Permission.type == 'read'))
# %%
groups.search(Group.permissions.all(Permission.type == 'read'))

# %%
db.search(User.name.one_of(['jane', 'John']))

# %%
db.all()
# %%
