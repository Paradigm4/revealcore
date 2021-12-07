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
  con_core$username
}

#' List all users
#'
#' @export
list_users = function(con) {
  iquery(con$db,
         "project(list('users'), id, name)",
         return=T,
         only_attributes=T,
         schema="<user_id:int64, user_name:string>[i]")
}

#' Add a user to a role
#' Run only with operator or admin privileges.  Admin priviledges required to grant operator or admin privileges.
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

#' Function to check if current user has admin permissions
#'
#' @param con
#'
#' @export
check_user_admin_status = function(con){
  admin_roles = c('root', 'scidbadmin', 'admin')
  user_roles = revealgenomics:::show_roles_for_user(con=con)
  return(any(admin_roles %in% user_roles))
}

#' Function to check if current user has operator permissions
#'
#' @param con
#'
#' @export
check_user_operator_status = function(con){
  operator_roles = c('root', 'scidbadmin', 'admin', 'operator')
  user_roles = revealgenomics:::show_roles_for_user(con=con)
  return(any(operator_roles %in% user_roles))
}

#' Function to show rules for user
#' Operator or higher privileges required to examine roles for a user other than oneself
#'
#' @param con connection object
#' @param user_name User name of user to show roles for.  If NULL, show roles for currently logged in user
#'
#' @export
show_roles_for_user = function(con, user_name = NULL) {
  if(is.null(user_name)){
    user_name = get_logged_in_user(con)
  } else if(length(user_name) != 1) {
    stop("Must specify exactly one user or user_name must be NULL")
  }
  if(user_name != get_logged_in_user(con) && !(check_user_operator_status(con))){
    stop("Operator privileges required to examine another user's roles")
  }

  roles = iquery(con$db,paste0("show_roles_for_user('",user_name,"')"),return=T)$role

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

