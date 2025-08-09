class MultiDBRouter:
    def db_for_read(self, model, **hints):
        if model._meta.model_name == 'user':
            return 'users'
        elif model._meta.model_name == 'product':
            return 'products'
        elif model._meta.model_name == 'order':
            return 'orders'
        return None

    def db_for_write(self, model, **hints):
        return self.db_for_read(model, **hints)

    def allow_relation(self, obj1, obj2, **hints):
        return True

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        if model_name == 'user' and db == 'users':
            return True
        elif model_name == 'product' and db == 'products':
            return True
        elif model_name == 'order' and db == 'orders':
            return True
        return False
