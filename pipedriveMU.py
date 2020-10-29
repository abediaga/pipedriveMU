#!/usr/bin/env python
# coding: utf-8

# In[340]:


import datetime
import requests
#!pip install python-pipedrive
from pipedrive import Pipedrive
#!pip install pymysql
import pymysql


# In[341]:


class WooCommerce ():
    """A simple example class"""
   
    def __init__(self, host,user,passwd,db):
        self.miConexion = pymysql.connect( host=host, user= user, passwd=passwd, db=db )
    
    def __del__(self):
        self.miConexion.close()
        
    def get_last_order_datetime(self):
        database = self.miConexion.cursor()
        database.execute( "SELECT * FROM wp_posts WHERE post_type = 'shop_order' ORDER BY post_date DESC" )
        orders = database.fetchall()
        if not orders :
            last_order_datetime = None
        else :
            last_order_datetime = orders[0][2]
        return(last_order_datetime)
    
    def get_all_orders(self) :
        database = self.miConexion.cursor()
        database2 = self.miConexion.cursor()
        database.execute( "SELECT ID, post_author, post_date, post_status, post_type FROM wp_posts WHERE post_type = 'shop_order'" )
        orders = {}
        for elem in database.fetchall() :
            database2.execute( "SELECT * FROM wp_postmeta WHERE post_id = " + str(elem[0]))
            order = {}
            for elem2 in database2.fetchall() :
                order[elem2[2]] = elem2[3]
            orders[elem[0]] = order
        return(orders)
    
    def get_all_orders_fromdate(self, from_date) :
        database4 = self.miConexion.cursor()
        database4.execute( "SELECT * FROM wp_posts WHERE post_type = 'shop_order' and post_date > '" + str(from_date) + "' ORDER BY post_date DESC" )
        result4 = database4.fetchall()
        return(result4)

    def get_order_details(self, post_id):
        database5 = self.miConexion.cursor()
        database5.execute( "SELECT * FROM wp_postmeta WHERE post_id = " + str(post_id) + "" )
        order_details = {}
        for elem in database5.fetchall() :
            order_details[elem[2]] = elem[3]
        return order_details

    def get_customer_details(self, post_id) :
        customer_details = {}
        database4 = self.miConexion.cursor()
        database4.execute( "SELECT * FROM wp_postmeta WHERE post_id = " + str(post_id) + "" )
        result4 = database4.fetchall()
        if result4 :
            for elem in result4:
                customer_details[elem[2]] = elem[3]
        return(customer_details)
    
    def get_sent_contact_forms(self) :
        sent_contact_forms = []
        database4 = self.miConexion.cursor()
        database4.execute( "SELECT * FROM `wp_vxcf_leads` ORDER BY created DESC" )
        result4 = database4.fetchall()
        if result4 :
            for elem in result4:
                sent_contact_form = {}
                sent_contact_form["id"] = elem[0]
                sent_contact_form["form_id"] = elem[1]
                sent_contact_form["status"] = elem[2]
                sent_contact_form["type"] = elem[3]
                sent_contact_form["is_read"] = elem[4]
                sent_contact_form["is_star"] = elem[5]
                sent_contact_form["user_id"] = elem[6]
                sent_contact_form["ip"] = elem[7]
                sent_contact_form["browser"] = elem[8]
                sent_contact_form["screen"] = elem[9]
                sent_contact_form["os"] = elem[10]
                sent_contact_form["vis_id"] = elem[11]
                sent_contact_form["url"] = elem[12]
                sent_contact_form["meta"] = elem[13]
                sent_contact_form["created"] = elem[14]
                sent_contact_form["updated"] = elem[15]
                
                database5 = self.miConexion.cursor()
                database5.execute( "SELECT * FROM `wp_vxcf_leads_detail` WHERE lead_id = '" + str(sent_contact_form["id"]) + "'" )
                result5 = database5.fetchall()
                for elem2 in result5:
                    sent_contact_form[elem2[2]] = elem2[3]
                sent_contact_forms.append(sent_contact_form)
        return(sent_contact_forms)

    def get_sent_contact_forms_fromdate(self, last_deal_datetime) :
        sent_contact_forms = []
        database4 = self.miConexion.cursor()
        database4.execute( "SELECT * FROM `wp_vxcf_leads` WHERE created > '" + str(last_deal_datetime) + "' ORDER BY created DESC" )
        result4 = database4.fetchall()
        if result4 :
            for elem in result4:
                sent_contact_form = {}
                sent_contact_form["id"] = elem[0]
                sent_contact_form["form_id"] = elem[1]
                sent_contact_form["status"] = elem[2]
                sent_contact_form["type"] = elem[3]
                sent_contact_form["is_read"] = elem[4]
                sent_contact_form["is_star"] = elem[5]
                sent_contact_form["user_id"] = elem[6]
                sent_contact_form["ip"] = elem[7]
                sent_contact_form["browser"] = elem[8]
                sent_contact_form["screen"] = elem[9]
                sent_contact_form["os"] = elem[10]
                sent_contact_form["vis_id"] = elem[11]
                sent_contact_form["url"] = elem[12]
                sent_contact_form["meta"] = elem[13]
                sent_contact_form["created"] = elem[14]
                sent_contact_form["updated"] = elem[15]
                
                database5 = self.miConexion.cursor()
                database5.execute( "SELECT * FROM `wp_vxcf_leads_detail` WHERE lead_id = '" + str(sent_contact_form["id"]) + "'" )
                result5 = database5.fetchall()
                for elem2 in result5:
                    sent_contact_form[elem2[2]] = elem2[3]
                sent_contact_forms.append(sent_contact_form)
        return(sent_contact_forms)


# In[342]:


class PipedriveMU ():
    """A simple example class"""
    api_key = 0    
    
    def __init__(self, api_key):
        self.pipedrive = Pipedrive(api_key)
        self.api_key = api_key
        
    
    def get_organization(self) :
        result = self.pipedrive.organizations(method='GET')
        return(result["data"][0])
    
    def get_pipelines(self) :
        result = self.pipedrive.pipelines(method='GET')
        return(result["data"])

    def get_pipeline(self, pipeline_name) :
        pipelines = self.pipedrive.pipelines(method='GET')
        if pipelines :
            for pipeline in pipelines["data"] :
                if pipeline["name"] == pipeline_name :
                    return(pipeline)

    def get_all_stages(self) :
        result = self.pipedrive.stages(method='GET')
        return(result["data"])

    def get_stages_pipeline(self, pipeline_id) :
        result = self.pipedrive.stages({
            "pipeline_id" : pipeline_id
        },method='GET')
        return(result["data"])

    def get_stage_frompipeline(self, pipeline_id, stage_name) :
        stages = self.pipedrive.stages({
            "pipeline_id" : pipeline_id
        },method='GET')
        if stages :
            for stage in stages["data"] :
                if stage["name"] == stage_name :
                    return(stage)

    def get_all_deals(self) :
        deals = self.pipedrive.deals(method='GET')
        return(deals["data"])

    def get_deals_stage(self, stage_id) :
        deals = self.pipedrive.deals({
            "stage_id" : stage_id
        },method='GET')
        return(deals["data"])

    def get_deals_pipeline(self, pipeline_id) :
        deals_pipeline = []
        stages_pipeline = get_stages_pipeline(pipeline_id)
        for stage in stages_pipeline :
            deals = self.get_deals_stage(stage["id"])
            if deals :
                for deal in deals :
                    deals_pipeline.append(deal)
        return(deals_pipeline)

    #Se podría crear otra función que devuelva los deals por cada stage
    #def get_deals_oderby_stage_pipeline(pipeline_id)
    
    def add_new_deal(self, deal_name, deal_value, pipeline, stage):
        new_deal = self.pipedrive.deals({
            'title': deal_name,
            'value': deal_value,
            'pipeline_id': pipeline,
            'stage_id': stage,
            'status': 'open'
        }, method='POST')
        return(new_deal)

    def get_deal (self, deal_id) :
        r_deal = ""
        deals = self.pipedrive.deals(method='GET')
        if deals["data"] :
            for deal in deals["data"] :
                if deal["id"] == deal_id :
                    r_deal = deal
        return(r_deal)

    #Obtener la fecha del último deal
    def get_last_deal_datetime(self) :
        deals = self.pipedrive.deals(method='GET')
        if not deals["data"] :
            last_deal_datetime = "0001-01-01 00:00:00"
            last_deal_datetime = datetime.datetime.strptime(last_deal_datetime, '%Y-%m-%d %H:%M:%S')
        else :
            num_deals = len(deals["data"])
            last_deal_datetime = deals["data"][num_deals-1]["add_time"]
            last_deal_datetime = datetime.datetime.strptime(last_deal_datetime, '%Y-%m-%d %H:%M:%S')
        return last_deal_datetime

    def get_last_deal_pipeline_datetime(self, funnel_name) :
        pipeline_id = pipedrive_bdata.get_pipeline(funnel_name)["id"]
        
        deals_pipeline = []
        
        result = self.pipedrive.stages({
            "pipeline_id" : pipeline_id
        },method='GET')
        stages_pipeline = result["data"]
    
        for stage in stages_pipeline :
            deals_ = self.pipedrive.deals({
                "stage_id" : stage["id"]
            },method='GET')
            deals = deals_["data"]
            if deals :
                for deal in deals :
                    deals_pipeline.append(deal)
        last_deal_datetime = "0001-01-01 00:00:00"
        if len(deals_pipeline) > 0 :
            for deal in deals_pipeline :
                if last_deal_datetime < deal["add_time"] :
                    last_deal_datetime = deal["add_time"]
        
        return(last_deal_datetime)
    
    def get_all_contacts (self) :
        result = self.pipedrive.persons(method='GET')
        return(result["data"])

    def get_contact_detail (self, contact_id) :
        contact_details = {}
        contacts = self.pipedrive.persons(method='GET')
        if contacts["data"] :
            for contact in contacts["data"] :
                if contact["id"] == contact_id :
                    contact_details = contact
        return(contact_details)

    def get_contact_id_email (self, email) :
        contact_id = 0
        contacts = self.pipedrive.persons(method='GET')
        if contacts["data"] :
            for contact in contacts["data"] :
                if contact["email"][0]["value"] == email :
                    contact_id = contact["id"]
        return(contact_id)

    def add_contact (self, c_owner_id, c_org_id, c_first_name, c_last_name, c_phone, c_email) :
        new_contact = self.pipedrive.persons({
            'owner_id': c_owner_id,
            'org_id': c_org_id,
            'name' : c_first_name + " " + c_last_name,
            'first_name': c_first_name,
            'last_name': c_last_name,
            'phone': c_phone,
            'email': c_email,
        }, method='POST')
        return(new_contact)    

    def assign_contact_to_deal (self, deal_id, contact_id) :
        API_URL = "https://api.pipedrive.com/v1/deals/"+ str(deal_id) +"?api_token=" + self.api_key
        #print(API_URL)
        data = {
            "person_id": contact_id
        }

        response = requests.put(API_URL, data)

        return(response.json())

    def add_activity_deal (self, company_id, user_id, deal_id, _type, activity_name, assigned_to_user_id) :
        #Asignar una actividad al DEAL (negocio) recientemente creado
        activity = self.pipedrive.activities({'company_id': company_id,
           'user_id': user_id,
           'deal_id': deal_id,
           'type': _type,
           'subject': activity_name,
           'assigned_to_user_id': assigned_to_user_id
        }, method='POST')
        return(activity["data"])

    def get_all_activities (self) :
        activities = self.pipedrive.activities(method='GET')
        return(activities["data"])

    def get_activities_deal (self,deal_id) :
        activities_deal = []
        activities = self.pipedrive.activities(method='GET')
        if activities["data"] :
            for activity in activities["data"] :
                if activity["deal_id"] == deal_id :
                    activities_deal.append(activity)
        return(activities_deal)

    def get_activities_user (self,user_id) :
        activities = self.pipedrive.activities({
            "user_id" : user_id
        },method='GET')
        return(activities["data"])

    #def get_activities_user_date (user_id, start_date, end_date):
    #def get_activities_date (start_date, end_date):
    
    def get_custom_field_key(self, custom_field_name):
        #Obtenemos todos los fields de un DEAL
        result = self.pipedrive.dealFields(method='GET')
        #Si exste el FIELD entonces le extraemos la key que nos servira cuando creemos un nuevo deal
        field_key = ""
        if result["data"] :
                for elem in result["data"] :
                    if elem["name"] == custom_field_name :
                        field_key = elem["key"]
        return(field_key)

    def add_custom_field_to_deals(custom_field_name):
        #Obtenemos todos los fields de un DEAL
        result = self.pipedrive.dealFields(method='GET')
        #Si exste el FIELD entonces le extraemos la key que nos servira cuando creemos un nuevo deal
        field_key = ""
        if result["data"] :
                for elem in result["data"] :
                    if elem["name"] == custom_field_name :
                        field_key = elem["key"]
        # Si no existe "" entonces lo creamos y obtenemos su key
        if field_key == "" :
            pipedrive.dealFields({
                'name': custom_field_name,
                'field_type': 'varchar',
                'active_flag': True, 
                'edit_flag': True, 
                'index_visible_flag': True, 
                'details_visible_flag': True, 
                'add_visible_flag': True, 
                'important_flag': True, 
                'bulk_edit_allowed': True, 
                'searchable_flag': False, 
                'filtering_allowed': True, 
                'sortable_flag': True, 
                'mandatory_flag': False,
                'custom_field' : True
            }, method='POST')
            result2 = self.pipedrive.dealFields(method='GET')
            for elemm in result2["data"] :
                if elemm["name"] == custom_field_name :
                    field_key = elemm["key"]
        return(field_key)    
    


# In[343]:


def sync_woocommerce_pipedrive (database_data, pipedrive_data) :
    wooCommerce_bdata = WooCommerce(database_data["host"],database_data["user"],database_data["passwd"],database_data["db"])
    pipedrive_bdata = PipedriveMU(pipedrive_data["api_token"])
    
    
    last_deal_datetime = pipedrive_bdata.get_last_deal_pipeline_datetime(pipedrive_data["orders_funnel_name"])
    orders = wooCommerce_bdata.get_all_orders_fromdate(last_deal_datetime)
    
    if orders :
        for order in orders:
            order_id = order[0]
            
            order_details = wooCommerce_bdata.get_order_details(order_id)
            
            deal_name = "Pedido ID " + str(order_id)
            deal_value = order_details["_order_total"]
            
            pipeline_id = pipedrive_bdata.get_pipeline(pipedrive_data["orders_funnel_name"])["id"]
            
            stage_id = pipedrive_bdata.get_stage_frompipeline(pipeline_id,pipedrive_data["order_to_stage"])["id"]
            
            nuevo_deal = pipedrive_bdata.add_new_deal(deal_name, deal_value, pipeline_id, stage_id)
            
            customer_details = wooCommerce_bdata.get_customer_details(order_id)
            
            organization = pipedrive_bdata.get_organization()
            
            c_owner_id = organization["owner_id"]["id"]
            c_org_id = organization["id"]
            c_first_name = customer_details["_billing_first_name"]
            c_last_name = customer_details["_billing_last_name"]
            c_phone = customer_details["_billing_phone"]
            c_email = customer_details["_billing_email"]
            
            id_contacto = pipedrive_bdata.get_contact_id_email(c_email)
            if id_contacto == 0 :
                new_contact = pipedrive_bdata.add_contact(c_owner_id, c_org_id, c_first_name, c_last_name, c_phone, c_email)
                #pipedrive_bdata.assign_contact_to_deal(nuevo_deal["data"]["id"], new_contact["id"])
                pipedrive_bdata.assign_contact_to_deal(nuevo_deal["data"]["id"], pipedrive_bdata.get_contact_id_email(c_email))
            else :
                #asignar contacto
                pipedrive_bdata.assign_contact_to_deal(nuevo_deal["data"]["id"], id_contacto)
            
            
            activity_name = "Validar pedido ID " + str(order_id)
            organization_id = organization["company_id"]
            
            pipedrive_bdata.add_activity_deal(organization_id, c_owner_id, nuevo_deal["data"]["id"], "task", activity_name, c_owner_id)
    
    last_deal_datetime = pipedrive_bdata.get_last_deal_pipeline_datetime(pipedrive_data["contact_form_funnel_name"])
    contact_forms = wooCommerce_bdata.get_sent_contact_forms_fromdate(last_deal_datetime)
    if contact_forms :
        for contact_form in contact_forms:
            deal_name = "Formulario ID " + str(contact_form["id"])
            deal_value = 0
            pipeline_id = pipedrive_bdata.get_pipeline(pipedrive_data["contact_form_funnel_name"])["id"]
            stage_id = pipedrive_bdata.get_stage_frompipeline(pipeline_id,pipedrive_data["contactform_to_stage"])["id"]
            
            nuevo_deal = pipedrive_bdata.add_new_deal(deal_name, deal_value, pipeline_id, stage_id)
            
            organization = pipedrive_bdata.get_organization()
            c_owner_id = organization["owner_id"]["id"]
            c_org_id = organization["id"]
            
            id_contacto = pipedrive_bdata.get_contact_id_email(contact_form["your-email"])
            if id_contacto == 0 :
                new_contact = pipedrive_bdata.add_contact(c_owner_id, c_org_id, contact_form["your-name"], "", 0, contact_form["your-email"])
                pipedrive_bdata.assign_contact_to_deal(nuevo_deal["data"]["id"], pipedrive_bdata.get_contact_id_email(contact_form["your-email"]))
            else :
                pipedrive_bdata.assign_contact_to_deal(nuevo_deal["data"]["id"], id_contacto)
            
            activity_name = "Responder formulario ID " + str(contact_form["id"])
            organization_id = organization["company_id"]
            
            pipedrive_bdata.add_activity_deal(organization_id, c_owner_id, nuevo_deal["data"]["id"], "task", activity_name, c_owner_id)
    
        

