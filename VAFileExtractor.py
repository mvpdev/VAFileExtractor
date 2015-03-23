#!/usr/bin/env python

import sys, time, csv, logging, datetime, redis, pickle, mysql.connector
from xlrd import open_workbook
from Daemon import Daemon
import Configs  #import configs
class VAFileExtractor(Daemon):
    
    #connect to database
    def mysql_connect(self):
        logging.info("Connecting to mysql ...")
        try:
            cnx = mysql.connector.connect(**Configs.mysql)
            logging.info("successfully connected to mysql ..." )
            return cnx
        except Exception, e:
             logging.exception("Exception raised while connecting to mysql ...=> " + str(e))
    
    def redis_connect(self):
        logging.info("Connecting to redis server ...")
        try:  
            cnx = redis.StrictRedis(**Configs.redis)
            logging.info("successfully connected to redis ...")
            
            return cnx
        except Exception, e:
             logging.exception("Exception raised while connecting to redis ...=> " + str(e))    
     
     
    def get_active_sites(self):
        try:  
            active = 1
            cnx = self.mysql_connect()
            cursor = cnx.cursor()
            query = "SELECT * FROM va_sites WHERE status = %s" % active
            logging.debug("SITES QUERY ...=> " + str(query))  
            cursor.execute(query)
            
            return cursor.fetchall()
            cnx.close()
        except Exception, e:
             logging.exception("Exception raised while querying sites ...=> " + str(e))   
             cnx.close()   
          
    def get_pending_file_upload(self, site_id):
        try:  
            active = 1
            cnx = self.mysql_connect()
            cursor = cnx.cursor()
            query = "SELECT * FROM va_rawdatafiles WHERE siteid_id = %s AND status = %s AND (datecached IS NULL OR refreshcache = true)" % (site_id, active)
            logging.debug("FILES QUERY ...=> " + str(query))   
            cursor.execute(query)
            
            cnx.close()
            return cursor.fetchone()
        except Exception, e:
             logging.exception("Exception raised while querying va_rawdatafiles ...=> " + str(e))  
             cnx.close()    
    
    def update_file_upload(self, fileid):
        try:
            inactive = 2
            cnx = self.mysql_connect()
            cursor = cnx.cursor()
            cursor.execute ("UPDATE va_rawdatafiles SET status = %s, datecached = now(), refreshcache = false WHERE fileid = %s " % (inactive, fileid))
            cnx.commit()
            logging.info("file upload updated so that its not processed a second time ... ")  
            cnx.close()
        except Exception, e:
            logging.exception("Exception raised while updating va_rawdatafiles ...=> " + str(e))  
            cnx.close()    
    

    def update_site_with_file_upload(self, fileid, siteid):
        try:
            inactive = 2
            cnx = self.mysql_connect()
            cursor = cnx.cursor()
            cursor.execute ("UPDATE va_sites SET current_fileid = %s WHERE siteid = %s " % (fileid, siteid))
            cnx.commit()
            logging.info("va_sites updated with current_fileid ... ")  
            cnx.close()
        except Exception, e:
            logging.exception("Exception raised while updating va_sites with current_fileid ...=> " + str(e))  
            cnx.close()    
    
        
    
    def file_to_array(self, file_to_cache):  
        data = [] 
        try:
             logging.info("file_to_array start...")
             book = open_workbook(file_to_cache,on_demand=True)
             for name in book.sheet_names():
                if name:
                    logging.info("sheet name ..." + str(name))
                    worksheet = book.sheet_by_name(name)
                    num_rows = worksheet.nrows - 1
                    num_cells = worksheet.ncols - 1
                    curr_row = 2
                    while curr_row < num_rows:#each row
                        curr_row += 1
                        row = worksheet.row(curr_row)
                        curr_cell = -1
                        thisRow = []
                        while curr_cell < num_cells: #each cell
                            curr_cell += 1
                            cell_value = worksheet.cell_value(curr_row, curr_cell)
                            if isinstance(cell_value, unicode):
                                cell_value =  cell_value.encode('utf-8','ignore')
                            thisRow.append(str(cell_value))
                        theString = ','.join(thisRow)
                        logging.info("add row to list " + str(theString[:100] + "..."))
                        data.append(thisRow)
             logging.info("now return array ... ")
             return data
        except Exception, e:
             logging.exception("Exception raised converting the upload while to array ...=> " + str(e))  
             return None
            
          
          
     
       #book = open_workbook(data_directory + '/' + output_file,on_demand=True)   

    def doJob(self):
        logging.info("start job...")
        sites = self.get_active_sites()
        if(sites):
            logging.info("active sites exist, loop through them and check if data needs caching ...=> " + str(sites))
            for site in sites:
                site_id = site[0]
                site_name = site[2]
                site_datakey = site[3]
                logging.info("current site_id=>" + str(site_id) + ", site_name=>" + str(site_name))
                #check if there is a file that needs caching
                file_row = self.get_pending_file_upload(site_id)
                if(file_row):
                    fileid = file_row[0]
                    file_name = file_row[2]
                    datecached = file_row[8]
                    refreshcache = file_row[9]
                    logging.info("file found that needs caching: file_name=>" + str(file_name) + ", datecached=>" + str(datecached) + ", refreshcache=>" + str(refreshcache))
                    file_to_cache = Configs.data_directory + file_name
                    
                    file_to_array = self.file_to_array(file_to_cache)
                    if(file_to_array): #if pushed in array, commit to memcache
                        redis_cnx = self.redis_connect()
                        pickled_object = pickle.dumps(file_to_array)      
                        redis_cnx.set(site_datakey, pickled_object)  
                        #we are done here, so lets update the upload record as cached.
                        self.update_file_upload(fileid) 
                        self.update_site_with_file_upload(fileid, site_id)
                                            
    def run(self):
        try:            
           logging.basicConfig(filename=Configs.logPath, level=logging.DEBUG, format='%(levelname)s: %(asctime)s%(message)s on line %(lineno)d')
        except Exception as e:
            print str(e) 
            
        while True:
            self.doJob()
            time.sleep(1)

            
if __name__ == "__main__":
        daemon = VAFileExtractor('/tmp/vafileectractor.pid')
        if len(sys.argv) == 2:
                if 'start' == sys.argv[1]:
                        daemon.start()
                elif 'stop' == sys.argv[1]:
                        daemon.stop()
                elif 'restart' == sys.argv[1]:
                        daemon.restart()
                else:
                        print "Unknown command"
                        sys.exit(2)
                sys.exit(0)
        else:
                print "usage: %s start|stop|restart" % sys.argv[0]
                sys.exit(2)
