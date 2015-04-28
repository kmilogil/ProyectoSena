<%-- 
    Document   : cargarPermisos
    Created on : 11/03/2015, 07:06:50 PM
    Author     : Akashyro
--%>
<%@page import="modulo.usuarios.FUsuario"%>
<%@page import="modulo.usuarios.dao.PermisoDao"%>
<%@page import="modulo.usuarios.dto.PermisoDto"%>
<%@page import="modulo.usuarios.dto.RolDto"%>
<%@page import="java.util.ArrayList"%>

<%@page contentType="text/html" pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <title>Permisos</title>
    </head>
    <body>
        <%
            ArrayList<PermisoDto> listaPermisos;
            FUsuario faUsu = new FUsuario();

            if (request.getParameter("idRol") == null) {

                listaPermisos = (ArrayList<PermisoDto>) faUsu.obtenerPermisosPorRol(1);

                if (listaPermisos.size() > 0) {
                    for (PermisoDto temPermiso : listaPermisos) {
                        out.write("<li role='presentation' class='text-left'><a href='" + temPermiso.getUrl() + "'>" + temPermiso.getPermiso() + "</a></li>");
                    }
                }
            }
        %>
    </body>
</html>


