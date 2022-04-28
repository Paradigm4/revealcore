#BEGIN_COPYRIGHT
#
# PARADIGM4 INC.
# This file is distributed along with the Paradigm4 Enterprise SciDB
# distribution kit and may only be used with a valid Paradigm4 contract
# and in accord with the terms and conditions specified by that contract.
#
# Copyright (C) 2010 - 2018 Paradigm4 Inc.
# All Rights Reserved.
#
#END_COPYRIGHT

#' Report logged in user
#'
#' @export
get_logged_in_user = function(con) {
  con$username
}

#' List all users
#'
#' list the full list of users registered with scidb if the current user is an operator.  Otherwise, check a list of users that may be maintained by the package schema
#'
#' @export
list_users = function(pkgEnv, con){
  if(check_user_operator_status(con = con)){
    iquery(con$db,
           "project(list('users'), id, name)",
           return=T,
           only_attributes=T,
           schema="<user_id:int64, user_name:string>[i]")
  }
  else {
    if(!is.null(pkgEnv$meta$L$package$package_user_list_entity)){
      message("Current user not an operator.  Checking list of package registered users rather than database authoritative list.  Run as operator for full user list.")
      iquery(con$db, full_arrayname(pkgEnv$meta$arrUserList), T)
    } else {
      stop("Listing users requires operator permissions.")
    }
  }
}

#' set secure_scan access permissions for a user
#'
#' to be run only by scidbadmin, or user
#' with Read / Write capability to \link{PERMISSIONS_ARRAY} and secured namespaces
#'
#' can supply multiple ids at a time.
#'
#' @param user_id the id of the user for whom to change permissions.  Obtained via \link{list_users}, or `-1` for all users (on Scidb 21.8 or above)
#' @param secure_id the ids along the secure dimension to change permissions for.
#'
#' @export
set_permissions = function(pkgEnv, con, user_id, secure_id, allowed, namespace=NULL){
  if(length(user_id) != 1)
  {
    stop("Must specify exactly one user")
  }
  if(length(secure_id) < 1)
  {
    stop("Must specify 1 or more datasets")
  }
  if(!(user_id==-1 & con$aop_connection$scidb_version()$major>=21)){
    users = list_users(pkgEnv, con = con)
    if(!(user_id %in% users$user_id)){
      stop("No user with specified id found.")
    }
  }
  if( allowed == FALSE )
  {
    permission='false'
  } else
  {
    permission='true'
  }
  if(!is.null(namespace) && !is.null(pkgEnv$meta$L$namespace[[namespace]]$secure_dimension)){
    secure_dimension = pkgEnv$meta$L$namespace[[namespace]]$secure_dimension
  } else {
    secure_dimension = pkgEnv$meta$L$package$secure_dimension
  }
  secure_id_str = paste0("[", paste0("(", sprintf("%.0f", secure_id), ")", collapse=",") ,"]")
  iquery(con$db, paste0(
    "insert(
      redimension(
       apply(
        build(<",secure_dimension,":int64>[i=0:*], '",secure_id_str,"', true),
         user_id,", sprintf("%.0f", user_id), ",
         access,", permission, "
        ),
        ", PERMISSIONS_ARRAY(pkgEnv, con, namespace),"
       ),
       ", PERMISSIONS_ARRAY(pkgEnv, con, namespace),"
      )"))
  max_version = max(iquery(con$db, sprintf("versions(%s)", PERMISSIONS_ARRAY(pkgEnv, con, namespace)), return=TRUE)$version_id)
  iquery(con$db, sprintf("remove_versions(%s, %i)", PERMISSIONS_ARRAY(pkgEnv, con, namespace), max_version))
}

#' Add a user to a role
#' Run only with operator or admin privileges.  Admin privileges required to grant operator or admin privileges.
#'
#' @param user_name username of user to add to role
#' @param role name of role to add user to.  See \link{show_roles}
#' @param add_to_inferior_roles if TRUE (default), also add user to any roles with lesser level than the specified role.  See \link{show_roles}
#'
#' @export
add_user_to_role = function(pkgEnv, con, user_name, role, add_to_inferior_roles=TRUE){
  rg_roles = show_roles(pkgEnv, con)
  user_roles = show_roles_for_user(con, user_name)
  if(!(role %in% rg_roles$role)){
    stop("Use only to manage roles for schema.  See `show_roles`.")
  }
  target_role = rg_roles[match(role, rg_roles$role),]
  if(!(target_role$role_initialized)){
    stop("Role has not been initialized.  See `show_roles`.")
  }
  if(!(ifelse(target_role$special_role, check_user_admin_status(con=con), check_user_operator_status(con=con)))){
    stop("Logged in user lacks permissions to add role.")
  }
  user_level = check_user_operator_status(con = con)
  rg_roles = rg_roles[rg_roles$role_initialized,]
  roles_to_add = c(role)
  if(add_to_inferior_roles){
    roles_to_add = unique(c(roles_to_add, rg_roles[rg_roles$level<target_role$level,"role"]))
  }
  roles_to_add = setdiff(roles_to_add, user_roles)
  if(length(roles_to_add)==0){
    message("No roles need to be added to user.")
  } else {
    for(role in roles_to_add){
      if(!is.null(pkgEnv$meta$L$role[[role]]$add_function)){
        do.call(eval(parse(text=pkgEnv$meta$L$role[[role]]$add_function)),
                args=list(con=con, user_name=user_name))
      } else {
        add_user_to_role_call(user_name = user_name, role = role, con = con)
      }
    }
  }
}

#' Wrapper for the SciDB add_user_to_role, with error checking
#'
#' @export
add_user_to_role_call = function(con, user_name, role){
  query = paste0("add_user_to_role('",user_name,"', '",role,"')")
  status = try({iquery(con$db, query)}, silent = TRUE) # Till https://paradigm4.atlassian.net/browse/SDB-6563 is fixed
  if (is.null(status)) {
    message("User added successfully to '", role,
            "' role")
  }
  else if (class(status) == 'try-error') {
    if (length(grep("already exists", status, value = T)) == 1) {
      message("User has already been added to '", role,
              "' role")
    } else {
      stop(paste0("Unexpected error adding user to '",role,"' role: "), status)
    }
  }
  else {
    stop("Expected status to be NULL or of class try-error")
  }
}

#' Drop a user from a role
#' Run only with operator or admin privileges.  Admin privileges required to remove operator or admin privileges.
#'
#' @param user_name username of user to drop from role
#' @param role name of role to drop user to.  See \link{show_roles}
#' @param drop_from_superior_roles if TRUE (default), also add user to any roles with greater or equal level to the specified role.  See \link{show_roles}
#'
#' @export
drop_user_from_role = function(pkgEnv, con, user_name, role, drop_from_superior_roles=TRUE){
  rg_roles = show_roles(pkgEnv, con)
  user_roles = show_roles_for_user(con=con, user_name = user_name)
  if(!(role %in% rg_roles$role)){
    stop("Use only to manage roles for schema.  See `show_roles`.")
  }
  target_role = rg_roles[match(role, rg_roles$role),]
  if(!(target_role$role_initialized)){
    stop("Role has not been initialized.  See `show_roles`.")
  }
  if(!(ifelse(target_role$special_role, check_user_admin_status(con=con), check_user_operator_status(con=con)))){
    stop("Logged in user lacks permissions to drop role.")
  }
  rg_roles = rg_roles[rg_roles$role_initialized,]
  remove_roles = c(role)
  if(drop_from_superior_roles){
    remove_roles = unique(c(remove_roles, rg_roles[rg_roles$level>=target_role$level,"role"]))
  }
  remove_roles = intersect(remove_roles, user_roles)
  if(length(remove_roles)==0){
    message("No roles need to be removed from user.")
  } else {
    if(drop_from_superior_roles & any(rg_roles[match(remove_roles, rg_roles$role),"special_role"]) & !(check_user_admin_status(con=con))){
      stop("Current user lacks permissions to remove some additional roles.  Run as admin user or without `drop_from_superior_roles`.")
    }
    for(role in remove_roles) {
      drop_user_from_role_call(con, user_name, role)
    }
  }

}

#' Wrapper for the SciDB drop_user_from_role, with error checking
#'
#' @export
drop_user_from_role_call = function(con, user_name, role){
  query = paste0("drop_user_from_role('",user_name,"', '",role,"')")
  status = try({iquery(con$db, query)}, silent = TRUE)
  if (is.null(status)) {
    message("User dropped successfully from '", role, "' role")
  }
  else if (class(status) == 'try-error') {
    stop(paste0("Unexpected error dropping user from '",role,"' role: "), status)
  }
  else {
    stop("Expected status to be NULL or of class try-error")
  }
}

#' Function to show roles associated with a schema
#'
#' @param pkgEnv a REVEAL package environment object
#' @param con if not null, display whether or not role has been initialized in DB
#'
#' @export
show_roles = function(pkgEnv, con = NULL) {
  exists = as.character(NA)
  if(!is.null(con)){
    roles_db = scidb::iquery(con$db, "list('roles')", T)
    exists = names(pkgEnv$meta$L$role) %in% roles_db$name
  }
  data.frame(role=names(pkgEnv$meta$L$role),
             level = sapply(pkgEnv$meta$L$role, function(x){x$level}),
             special_role = sapply(pkgEnv$meta$L$role, function(x){x$special_role}),
             role_initialized = exists,
             description=sapply(pkgEnv$meta$L$role, function(x){x$docstring}),
             stringsAsFactors = FALSE,
             row.names = NULL)
}

#' @export
show_role_permissions = function(pkgEnv, role){
  perms = pkgEnv$meta$L$role[[role]]$namespace_permissions
  if(is.null(perms)){
    perms = data.frame("namespace"=character(), "permissions"=character())
  } else {
    perms = data.frame("namespace"=names(perms), "permissions"=unlist(perms))
  }
  perms
}

#' Function to check if current user has admin permissions
#'
#' @param con a connection object
#'
#' @export
check_user_admin_status = function(con, user_name = get_logged_in_user(con)){
  admin_roles = c('root', 'scidbadmin', 'admin')
  user_roles = show_roles_for_user(con=con, user_name=user_name)
  return(any(admin_roles %in% user_roles))
}

#' Function to check if current user has operator permissions
#'
#' @param con a connection object
#'
#' @export
check_user_operator_status = function(con, user_name = get_logged_in_user(con)){
  operator_roles = c('root', 'scidbadmin', 'admin', 'operator')
  user_roles = show_roles_for_user(con=con, user_name=user_name)
  return(any(operator_roles %in% user_roles))
}

#' Function to show what permissions a user has to a namespace according to the schema
#'
#' @param con a connection object
#' @param user_name the user for whom to show permissions.  If `NULL` (default), use the current logged in user
#' @param namespace the namespace for which to show permissions
#'
#' @return a list with names `c("c", "r", "u", "l", "d")` with value `TRUE` if the user has the corresponding permission in the specified namespace
#'
#' @export
show_user_namespace_permissions = function(pkgEnv, con, user_name = get_logged_in_user(con), namespace){
  if(check_user_operator_status(con=con, user_name = user_name)){
    return(list("c"=T,"r"=T,"u"=T,"l"=T,"d"=T))
  }
  roles = show_roles_for_user(con = con, user_name = user_name)
  roles = intersect(names(pkgEnv$meta$L$role), roles)
  role_permissions = paste(unlist(sapply(roles, function(x){pkgEnv$meta$L$role[[x]]$namespace_permissions[[namespace]]})), collapse="")
  if(length(role_permissions)==0){
    return(list("c"=F,"r"=F,"u"=F,"l"=F,"d"=F))
  }
  as.list(sapply(c("c","r","u","l","d"), function(x){grepl(x, role_permissions, fixed=T)}))
}

#' Function to show rules for user
#' Operator or higher privileges required to examine roles for a user other than oneself
#'
#' @param con connection object
#' @param user_name User name of user to show roles for.  If NULL, show roles for currently logged in user
#'
#' @export
show_roles_for_user = function(con, user_name = get_logged_in_user(con)) {
  if(length(user_name) != 1) {
    stop("Must specify exactly one user or user_name must be NULL")
  }
  if(con$scidb_version$major >= 17 & user_name=='root'){username='scidbadmin'}
  if(user_name != get_logged_in_user(con)){
    if(!(check_user_operator_status(con))){
      stop("Operator privileges required to examine another user's roles")
    }
    roles = iquery(con$db,paste0("show_roles_for_user('",user_name,"')"),return=T)$role
  } else{
    if(is.null(con$roles)){
      roles = iquery(con$db,paste0("show_roles_for_user('",user_name,"')"),return=T)$role
    } else {
      roles = con$roles
    }
  }

  return(roles)
}

#' Show users with a specified role
#' Run only with operator or higher privileges
#'
#' @param con connection object (optional when using \code{rg_connect()})
#' @param role Name of role to generate list of users for
#'
#' @export
show_users_in_role = function(con, role) {
  if(!(check_user_operator_status(con))){
    stop("Run only with operator privilege")
  }
  if(length(role) != 1) {
    stop("Must specify exactly one role")
  }
  return(iquery(con$db,paste0("show_users_in_role('",role,"')"),return=T)$user)
}

