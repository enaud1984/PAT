import datetime


class DbLog():

    def truncateLogTable(self):
        pass

    def dropLogTable(self):
        pass

    def create_log_table(self):
        pass

    def createLogTableHdfs(self):
        # solo per postgres
        query = "CREATE TABLE {} ".format(self.log_table_hdfs)
        query += "(RUN_ID character varying(128) NOT NULL, FILE_HDFS character varying(300) NOT NULL)"
        print("CreateLogTableHdfs:", query)
        self.db.execute_query(query)

        # param contiene la lista dei parametri input (fields) e la lista dei valori (tuples)

    ########################################################################
    ####################              log               ####################
    ########################################################################

    def get_param(
            self,
            p1,
            p2,
            ignore_null=False
    ):
        for idx, val in reversed(list(enumerate(p1))):
            if ignore_null and p2[idx] is None:
                p1.pop(idx)
                p2.pop(idx)
            elif val == 'self':
                p1.pop(idx)
                p2.pop(idx)
        return [p1, p2]

    def log_insert(
            self,
            run_id=None,
            script=None,
            description=None,
            execution_time=None,
            success=1,
            start_time=None,
            end_time=None,
            expiration_date=None,
            exit_code=None,
            app_id=None,
            log_path=None
    ):
        param = self.get_param(list(locals().keys()), list(locals().values()))
        print(param)

    def log_update(
            self,
            run_id=None,
            script=None,
            description=None,
            execution_time=None,
            success=1,
            start_time=None,
            end_time=None,
            expiration_date=None,
            exit_code=None,
            app_id=None,
            log_path=None
    ):
        param = self.get_param(list(locals().keys()), list(locals().values()))
        print(param)

    def log_select(
            self,
            run_id=None,
            script=None,
            description=None,
            execution_time=None,
            success=1,
            start_time=None,
            end_time=None,
            expiration_date=None,
            exit_code=None,
            app_id=None,
            log_path=None
    ):
        param = self.get_param(list(locals().keys()), list(locals().values()))
        print(param)

    ########################################################################
    ####################         log_operation          ####################
    ########################################################################

    def log_operation_insert(
            self,
            run_id=None,
            operation=None,
            start_time=None,
            end_time=None,
            exit_code=None
    ):
        param = self.get_param(list(locals().keys()), list(locals().values()))
        print(param)

    def log_operation_update(
            self,
            run_id=None,
            operation=None,
            start_time=None,
            end_time=None,
            exit_code=None
    ):
        param = self.get_param(list(locals().keys()), list(locals().values()))
        print(param)

    def log_operation_select(
            self,
            run_id=None,
            operation=None,
            start_time=None,
            end_time=None,
            exit_code=None
    ):
        param = self.get_param(list(locals().keys()), list(locals().values()))
        print(param)

    ########################################################################
    ####################         log_exit_code          ####################
    ########################################################################

    def log_exit_code_insert(
            self,
            type=None,
            message=None,
    ):
        param = self.get_param(list(locals().keys()), list(locals().values()))
        print(param)

    def log_exit_code_update(
            self,
            exit_code=None,
            type=None,
            message=None,
    ):
        param = self.get_param(list(locals().keys()), list(locals().values()))
        print(param)

    def log_exit_code_select(
            self,
            exit_code=None,
            type=None,
            message=None,
    ):
        param = self.get_param(list(locals().keys()), list(locals().values()))
        print(param)

    ########################################################################
    ####################           log_file             ####################
    ########################################################################

    def log_file_insert(
            self,
            run_id=None,
            filename=None,
            filepath=None,
            exit_code=None
    ):
        param = self.get_param(list(locals().keys()), list(locals().values()))
        print(param)

    def log_file_update(
            self,
            run_id=None,
            filename=None,
            filepath=None,
            exit_code=None
    ):
        param = self.get_param(list(locals().keys()), list(locals().values()))
        print(param)

    def log_file_select(
            self,
            run_id=None,
            filename=None,
            filepath=None,
            exit_code=None
    ):
        param = self.get_param(list(locals().keys()), list(locals().values()))
        print(param)
