#!/usr/bin/env python
# coding: utf-8

# In[165]:


import datetime
import requests
#!pip install python-pipedrive
from pipedrive import Pipedrive
#!pip install pymysql
import pymysql


# In[166]:


def get_connection_database (host,user,passwd,db) :
    miConexion = pymysql.connect( host=host, user= user, passwd=passwd, db=db )
    return(miConexion)

def close_connection_database (miConexion) :
    miConexion.close()


# In[167]:


#Obtener la fecha del último pedido
def get_last_woocommerce_order_datetime(miConexion) :
    database3 = miConexion.cursor()
    database3.execute( "SELECT * FROM wp_posts WHERE post_type = 'shop_order' ORDER BY post_date DESC" )
    result3 = database3.fetchall()
    if not result3 :
        last_order_datetime = None
    else :
        last_order_datetime = result3[0][2]
    return(last_order_datetime)

#Obtenemos todos los pedidos
def get_woocommerce_orders(miConexion) :
    database = miConexion.cursor()
    database2 = miConexion.cursor()
    database.execute( "SELECT ID, post_author, post_date, post_status, post_type FROM wp_posts WHERE post_type = 'shop_order'" )
    orders = {}
    for elem in database.fetchall() :
        database2.execute( "SELECT * FROM wp_postmeta WHERE post_id = " + str(elem[0]))
        order = {}
        for elem2 in database2.fetchall() :
            order[elem2[2]] = elem2[3]
        orders[elem[0]] = order
    return(orders)

def get_woocommerce_orders_fromdate(miConexion, from_date) :
    database4 = miConexion.cursor()
    database4.execute( "SELECT * FROM wp_posts WHERE post_type = 'shop_order' and post_date > '" + str(from_date) + "' ORDER BY post_date DESC" )
    result4 = database4.fetchall()
    return(result4)

def get_woocommerce_order_details(miConexion, post_id):
    miConexion = pymysql.connect( host='64.225.76.28', user= 'bdata3', passwd='BusinessAnalytics2.0', db='bdata50' )
    database5 = miConexion.cursor()
    database5.execute( "SELECT * FROM wp_postmeta WHERE post_id = " + str(post_id) + "" )
    order_details = {}
    for elem in database5.fetchall() :
        order_details[elem[2]] = elem[3]
    return order_details

def get_woocommerce_customer_details(miConexion, post_id) :
    customer_details = {}
    database4 = miConexion.cursor()
    database4.execute( "SELECT * FROM wp_postmeta WHERE post_id = " + str(post_id) + "" )
    result4 = database4.fetchall()
    if result4 :
        for elem in result4:
            customer_details[elem[2]] = elem[3]
    return(customer_details)


# In[168]:


def get_custom_field_key(custom_field_name):
    #Obtenemos todos los fields de un DEAL
    result = pipedrive.dealFields(method='GET')
    #Si exste el FIELD entonces le extraemos la key que nos servira cuando creemos un nuevo deal
    field_key = ""
    if result["data"] :
            for elem in result["data"] :
                if elem["name"] == custom_field_name :
                    field_key = elem["key"]
    return(field_key)

def add_custom_field_to_deals(custom_field_name):
    #Obtenemos todos los fields de un DEAL
    result = pipedrive.dealFields(method='GET')
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
        result2 = pipedrive.dealFields(method='GET')
        for elemm in result2["data"] :
            if elemm["name"] == custom_field_name :
                field_key = elemm["key"]
    return(field_key)


# In[169]:


def get_pipelines() :
    result = pipedrive.pipelines(method='GET')
    return(result["data"])

def get_pipeline(pipeline_name) :
    pipelines = pipedrive.pipelines(method='GET')
    if pipelines :
        for pipeline in pipelines["data"] :
            if pipeline["name"] == pipeline_name :
                return(pipeline)


# In[170]:


def get_all_stages() :
    result = pipedrive.stages(method='GET')
    return(result["data"])

def get_stages_pipeline(pipeline_id) :
    result = pipedrive.stages({
        "pipeline_id" : pipeline_id
    },method='GET')
    return(result["data"])

def get_stage_frompipeline(pipeline_id, stage_name) :
    stages = pipedrive.stages({
        "pipeline_id" : pipeline_id
    },method='GET')
    if stages :
        for stage in stages["data"] :
            if stage["name"] == stage_name :
                return(stage)


# In[171]:


def get_all_deals() :
    deals = pipedrive.deals(method='GET')
    return(deals["data"])

def get_deals_stage(stage_id) :
    deals = pipedrive.deals({
        "stage_id" : stage_id
    },method='GET')
    return(deals["data"])

def get_deals_pipeline(pipeline_id) :
    deals_pipeline = []
    stages_pipeline = get_stages_pipeline(pipeline_id)
    for stage in stages_pipeline :
        deals = get_deals_stage(stage["id"])
        if deals :
            for deal in deals :
                deals_pipeline.append(deal)
    return(deals_pipeline)

#Se podría crear otra función que devuelva los deals por cada stage
#def get_deals_oderby_stage_pipeline(pipeline_id)


# In[172]:


def add_activity_deal (company_id, user_id, deal_id, _type, activity_name, assigned_to_user_id) :
    #Asignar una actividad al DEAL (negocio) recientemente creado
    activity = pipedrive.activities({'company_id': company_id,
       'user_id': user_id,
       'deal_id': deal_id,
       'type': _type,
       'subject': activity_name,
       'assigned_to_user_id': assigned_to_user_id
    }, method='POST')
    return(activity["data"])

def get_all_activities () :
    activities = pipedrive.activities(method='GET')
    return(activities["data"])

def get_activities_deal (deal_id) :
    activities_deal = []
    activities = pipedrive.activities(method='GET')
    if activities["data"] :
        for activity in activities["data"] :
            if activity["deal_id"] == deal_id :
                activities_deal.append(activity)
    return(activities_deal)

def get_activities_user (user_id) :
    activities = pipedrive.activities({
        "user_id" : user_id
    },method='GET')
    return(activities["data"])

#def get_activities_user_date (user_id, start_date, end_date):
#def get_activities_date (start_date, end_date):


# In[173]:


#Obtener la fecha del último deal
def get_last_deal_datetime() :
    deals = pipedrive.deals(method='GET')
    if not deals["data"] :
        last_deal_datetime = "2000-01-01 00:00:00"
        last_deal_datetime = datetime.datetime.strptime(last_deal_datetime, '%Y-%m-%d %H:%M:%S')
    else :
        num_deals = len(deals["data"])
        last_deal_datetime = deals["data"][num_deals-1]["add_time"]
        last_deal_datetime = datetime.datetime.strptime(last_deal_datetime, '%Y-%m-%d %H:%M:%S')
    return last_deal_datetime

def add_new_deal(deal_name, deal_value, pipeline, stage):
    new_deal = pipedrive.deals({
        'title': deal_name,
        'value': deal_value,
        'pipeline_id': pipeline,
        'stage_id': stage,
        'status': 'open'
    }, method='POST')
    return(new_deal)

def get_deal (deal_id) :
    r_deal = ""
    deals = pipedrive.deals(method='GET')
    if deals["data"] :
        for deal in deals["data"] :
            if deal["id"] == deal_id :
                r_deal = deal
    return(r_deal)

def assign_contact_to_deal_v0 (deal_id, contact_id) :
    deal = get_deal(deal_id)
    contact = get_contact_detail(contact_id)
    new_deal = pipedrive.deals({
        'id' : deal_id,
        'person_id' : contact_id
    }, method='PUT')
    return(new_deal)

def assign_contact_to_deal (deal_id, contact_id) :
    API_URL = "https://api.pipedrive.com/v1/deals/"+ str(deal_id) +"?api_token=" + API_KEY
    #print(API_URL)
    data = {
        "person_id": contact_id
    }

    response = requests.put(API_URL, data)
    
    return(response.json())


# In[174]:


def get_all_contacts () :
    result = pipedrive.persons(method='GET')
    return(result["data"])

def get_contact_detail (contact_id) :
    contact_details = {}
    contacts = pipedrive.persons(method='GET')
    if contacts["data"] :
        for contact in contacts["data"] :
            if contact["id"] == contact_id :
                contact_details = contact
    return(contact_details)

def get_contact_id_email (email) :
    contact_id = 0
    contacts = pipedrive.persons(method='GET')
    if contacts["data"] :
        for contact in contacts["data"] :
            if contact["email"][0]["value"] == email :
                contact_id = contact["id"]
    return(contact_id)

def add_contact (c_owner_id, c_org_id, c_first_name, c_last_name, c_phone, c_email) :
    new_contact = pipedrive.persons({
        'owner_id': c_owner_id,
        'org_id': c_org_id,
        'name' : c_first_name + " " + c_last_name,
        'first_name': c_first_name,
        'last_name': c_last_name,
        'phone': c_phone,
        'email': c_email,
    }, method='POST')
    return(new_contact)


# In[175]:


def get_organization() :
    result = pipedrive.organizations(method='GET')
    return(result["data"][0])


# In[176]:

def get_connection_pipedrive(api_key) :
    pipedrive = Pipedrive(api_key)
    return (pipedrive)

def sync_woocommerce_pipedrive (database_data, pipedrive_data) :
	
    miConexion = get_connection_database(database_data["host"],database_data["user"],database_data["passwd"],database_data["db"])
    pipedrive = get_connection_pipedrive(pipedrive_data["API_KEY"])
	
    last_deal_datetime = get_last_deal_datetime()
    orders = get_woocommerce_orders_fromdate(miConexion, last_deal_datetime)
    if orders :
        for order in orders:
            order_id = order[0]
            order_details = get_woocommerce_order_details(miConexion, order_id)
            
			deal_name = "Pedido ID " + str(order_id)
            deal_value = order_details["_order_total"]
            
			pipeline_id = get_pipeline('Embudo Data Running')["id"]
            stage_id = get_stage_frompipeline(pipeline_id,"Pedidos realizados")["id"]
            nuevo_deal = add_new_deal(deal_name, deal_value, pipeline_id, stage_id)
            customer_details = get_woocommerce_customer_details(miConexion, order_id)
            organization = get_organization()
            c_owner_id = organization["owner_id"]["id"]
            c_org_id = organization["id"]
            c_first_name = customer_details["_billing_first_name"]
            c_last_name = customer_details["_billing_last_name"]
            c_phone = customer_details["_billing_phone"]
            c_email = customer_details["_billing_email"]
            new_contact = add_contact (c_owner_id, c_org_id, c_first_name, c_last_name, c_phone, c_email)
            assign_contact_to_deal(nuevo_deal["data"]["id"], get_contact_id_email(c_email))
            activity_name = "Revisar pedido ID " + str(order_id)
            organization_id = organization["company_id"]
            add_activity_deal(organization_id, c_owner_id, nuevo_deal["data"]["id"], "task", activity_name, c_owner_id)
    close_connection_database(miConexion)
        

