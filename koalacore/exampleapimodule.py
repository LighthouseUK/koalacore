__author__ = 'Matt'


class APIModule(object):
    """
    This is a boilerplate implementation of an API module which performs CRUD operations. It includes
    various hooks before and after an operation completes so that other modules can extend it's functionality.

    Module builders should pass all arguments to the 'pre' hook events in the same order as they were provided
    to method. The 'post' hook event should include the access_token and the result of whatever op was performed. In
    almost all cases, to be useful, the hook should include the model that was inserted. For example, when inserting a
    model into NDB the result is a key instance (if successful). This is not overly useful for any function that hooks
    into the post insert event. If we also send the data model we have a lot more options for post processing.

    ***IMPORTANT***: the first argument to the signal should always be 'cls'. This allows other code to subscribe
    specifically to events in this module.

    _datastore is an interface to whichever datastore is being used. See Koala.datastore for more information
    on how the interface should behave.

    The module should abstract returned values from any underlying implementations. For example, the standard
    datastore on GAE is NDB. NDB uses NDB models which contain methods/data specific to the datastore. In the
    interest of maintenance, modules, for example, should convert the NDB model to normal python objects. This
    allows you to change the implementation at a later date without breaking other API modules.

    You may notice the 'access_token' argument which is supplied to each method. I conceded that a stateless
    design and conforming to the UNIX philosophy was not possible in this particular case. 'access_token',
    while not relevant to anything outside of the 'Credentials' and 'Security' modules, still needs to be
    passed into the signal hooks so that it may be verified by other modules. Keeping a stateless design is
    paramount for testing and maintenance. Think you know a better solution? Please get in touch!

    """
    _datastore = NDBInterface

    # Signal constants
    HOOK_PRE_NEW = 'hook_pre_new'
    HOOK_POST_NEW = 'hook_post_new'
    HOOK_PRE_INSERT = 'hook_pre_insert'
    HOOK_POST_INSERT = 'hook_post_insert'
    HOOK_PRE_GET = 'hook_pre_get'
    HOOK_POST_GET = 'hook_post_get'
    HOOK_PRE_UPDATE = 'hook_pre_update'
    HOOK_POST_UPDATE = 'hook_post_update'
    HOOK_PRE_DELETE = 'hook_pre_delete'
    HOOK_POST_DELETE = 'hook_post_delete'
    HOOK_PRE_GET_MULTI = 'hook_pre_get_multi'
    HOOK_POST_GET_MULTI = 'hook_post_get_multi'
    HOOK_PRE_UPDATE_MULTI = 'hook_pre_update_multi'
    HOOK_POST_UPDATE_MULTI = 'hook_post_update_multi'

    # New method hooks
    hook_pre_new = signal(HOOK_PRE_NEW)
    _hook_pre_new_enabled = False
    hook_post_new = signal(HOOK_POST_NEW)
    _hook_post_new_enabled = False

    # Insert method hooks
    hook_pre_insert = signal(HOOK_PRE_INSERT)
    _hook_pre_insert_enabled = False
    hook_post_insert = signal(HOOK_POST_INSERT)
    _hook_post_insert_enabled = False

    # Get method hooks
    hook_pre_get = signal(HOOK_PRE_GET)
    _hook_pre_get_enabled = False
    hook_post_get = signal(HOOK_POST_GET)
    _hook_post_get_enabled = False

    # Update method hooks
    hook_pre_update = signal(HOOK_PRE_UPDATE)
    _hook_pre_update_enabled = False
    hook_post_update = signal(HOOK_POST_UPDATE)
    _hook_post_update_enabled = False

    # Delete method hooks
    hook_pre_delete = signal(HOOK_PRE_DELETE)
    _hook_pre_delete_enabled = False
    hook_post_delete = signal(HOOK_POST_DELETE)
    _hook_post_delete_enabled = False

    # Get Multi method hooks
    hook_pre_get_multi = signal(HOOK_PRE_GET_MULTI)
    _hook_pre_get_multi_enabled = False
    hook_post_get_multi = signal(HOOK_POST_GET_MULTI)
    _hook_post_get_multi_enabled = False

    # Update Multi method hooks
    hook_pre_update_multi = signal(HOOK_PRE_UPDATE_MULTI)
    _hook_pre_update_multi_enabled = False
    hook_post_update_multi = signal(HOOK_POST_UPDATE_MULTI)
    _hook_post_update_multi_enabled = False

    @classmethod
    def new(cls, access_token, **kwargs):
        """
        Create a new entity.

        :param access_token:
        :param kwargs:
        :return entity:
        """
        if cls._hook_pre_new_enabled:
            cls.hook_pre_new.send(cls, access_token=access_token, **kwargs)

        op_result = cls._datastore.new(**kwargs)

        if cls._hook_post_new_enabled:
            cls.hook_post_new.send(cls, access_token=access_token, op_result=op_result)

        return op_result

    @classmethod
    def insert(cls, access_token, entity):
        """
        Insert entity into the datastore.

        :param access_token:
        :param entity:
        :return inserted entity uid:
        """

        if cls._hook_pre_insert_enabled:
            cls.hook_pre_insert.send(cls, access_token=access_token, entity=entity)

        op_result = cls._datastore.insert(resource_object=entity)

        if cls._hook_post_insert_enabled:
            cls.hook_post_insert.send(cls, access_token=access_token, entity=entity, op_result=op_result)

        return op_result

    @classmethod
    def get(cls, access_token, uid):
        """
        Retrieve entity from the datastore.

        :param access_token:
        :param uid:
        :return entity:
        """

        if cls._hook_pre_get_enabled:
            cls.hook_pre_get.send(cls, access_token=access_token, uid=uid)

        op_result = cls._datastore.get(resource_uid=uid)

        if cls._hook_post_get_enabled:
            cls.hook_post_get.send(cls, access_token=access_token, op_result=op_result)

        return op_result

    @classmethod
    def get_multi(cls, access_token, uids):
        """
        Get multiple entities simultaneously
        :param access_token:
        :param uids (list of entity uids):
        :return list of entities:
        """

        if cls._hook_pre_get_multi_enabled:
            cls.hook_pre_get_multi.send(cls, access_token=access_token, uids=uids)

        op_result = cls._datastore.get_multi(resource_uids=uids)

        if cls._hook_post_get_multi_enabled:
            cls.hook_post_get_multi.send(cls, access_token=access_token, op_result=op_result)

        return op_result

    @classmethod
    def update(cls, access_token, entity):
        """
        Store updated entity in the datastore.

        :param access_token:
        :param entity:
        :return updated entity uid:
        """

        if cls._hook_pre_update_enabled:
            cls.hook_pre_update.send(cls, access_token=access_token, entity=entity)

        op_result = cls._datastore.update(resource_object=entity)

        if cls._hook_post_update_enabled:
            cls.hook_post_update.send(cls, access_token=access_token, entity=entity, op_result=op_result)

        return op_result

    @classmethod
    def update_multi(cls, access_token, entities):
        """
        Store updated entities in the datastore.

        :param access_token:
        :param entities (list of entity objects):
        :return updated entity uids:
        """

        if cls._hook_pre_update_multi_enabled:
            cls.hook_pre_update_multi.send(cls, access_token=access_token, entities=entities)

        op_result = cls._datastore.update_multi(resource_objects=entities)

        if cls._hook_post_update_multi_enabled:
            cls.hook_post_update_multi.send(cls, access_token=access_token, entities=entities, op_result=op_result)

        return op_result

    @classmethod
    def delete(cls, access_token, uid):
        """
        Delete entity from datastore.

        :param access_token:
        :param uid:
        """

        if cls._hook_pre_delete_enabled:
            cls.hook_pre_delete.send(cls, access_token=access_token, uid=uid)

        cls._datastore.delete(resource_uid=uid)

        if cls._hook_post_delete_enabled:
            cls.hook_post_delete.send(cls, access_token=access_token, resource_uid=uid)

    @classmethod
    def _build_sec_meta_uid(cls, resource_uid):
        return cls._datastore.build_resource_uid(desired_id=resource_uid)

    @classmethod
    def parse_signal_receivers(cls):
        """
        Check for subscribers to each hook, toggling the enabled flag accordingly.

        ***IMPORTANT*** This method changes the class definition itself. It should only be
        invoked the the top level class otherwise you will experience bugs where different
        implementations inherit the same set of flags.

        """
        # Toggle 'new' hooks
        cls._hook_pre_new_enabled = bool(cls.hook_pre_new.receivers)
        cls._hook_post_new_enabled = bool(cls.hook_post_new.receivers)

        # Toggle 'insert' hooks
        cls._hook_pre_insert_enabled = bool(cls.hook_pre_insert.receivers)
        cls._hook_post_insert_enabled = bool(cls.hook_post_insert.receivers)

        # Toggle 'get' hooks
        cls._hook_pre_get_enabled = bool(cls.hook_pre_get.receivers)
        cls._hook_post_get_enabled = bool(cls.hook_post_get.receivers)

        # Toggle 'update' hooks
        cls._hook_pre_update_enabled = bool(cls.hook_pre_update.receivers)
        cls._hook_post_update_enabled = bool(cls.hook_post_update.receivers)

        # Toggle 'delete' hooks
        cls._hook_pre_delete_enabled = bool(cls.hook_pre_delete.receivers)
        cls._hook_post_delete_enabled = bool(cls.hook_post_delete.receivers)

        # Toggle 'get_multi' hooks
        cls._hook_pre_get_multi_enabled = bool(cls.hook_pre_get_multi.receivers)
        cls._hook_post_get_multi_enabled = bool(cls.hook_post_get_multi.receivers)

        # Toggle 'update_multi' hooks
        cls._hook_pre_update_multi_enabled = bool(cls.hook_pre_update_multi.receivers)
        cls._hook_post_update_multi_enabled = bool(cls.hook_post_update_multi.receivers)
