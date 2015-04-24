<%-- 
    Document   : indexp
    Created on : 30/03/2015, 05:31:28 PM
    Author     : Akashyro
--%>

<%@page import="modulo.usuarios.dto.PermisoDto"%>
<%@page import="modulo.usuarios.FUsuario"%>
<%@page import="modulo.usuarios.dto.RolDto"%>
<%@page import="java.util.ArrayList"%>
<%@page import="modulo.usuarios.dto.UsuarioDto"%>
<%
    HttpSession miSesion = request.getSession(false);
    HttpSession miSesionRoles = request.getSession(false);

    UsuarioDto actualUsuario;
    ArrayList<RolDto> rolesActuales;

    actualUsuario = (UsuarioDto) miSesion.getAttribute("usuarioEntro");
    rolesActuales = (ArrayList<RolDto>) miSesionRoles.getAttribute("roles");

    if (actualUsuario == null) {
        response.sendRedirect("../index.jsp?msg=<strong><i class='glyphicon glyphicon-exclamation-sign'></i> ¡Ups!</strong> Inicie Sesión Primero.&tipoAlert=info");
    } else {
        RolDto primerRol = rolesActuales.get(0);

        FUsuario faUsu = new FUsuario();
        String pagActual = "indexp.jsp";

        // Validación para poder entrar
        boolean poderEntrar = false;

        for (RolDto rol : rolesActuales) {
            if (rol.getIdRol() == 1 || rol.getIdRol() == 2) {
                poderEntrar = true;
            }
        }

        if (poderEntrar) {

%>
<%@page contentType="text/html" pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <title>Inicio - Farmer's Market</title>
        <link rel="shortcut icon" href="../img/favicon.ico">
        <link rel="stylesheet" type="text/css" href="../css/bootstrap.css">
        <link rel="stylesheet" type="text/css" href="../css/font-awesome.css">
        <script type="text/javascript" src="../js/jquery-1.11.2.js"></script>
        <script type="text/javascript" src="../js/bootstrap.js"></script>
        <script type="text/javascript" src="../js/ajax.js"></script>
        <script type="text/javascript">
            $(document).ready(function() {
                // Initialize tooltip
                $('[data-toggle="tooltip"]').tooltip({
                    placement: 'top'
                });
            });
        </script>
    </head>
    <body>
        <div class="container ">
            <!-- Banner Farmer's Market -->
            <div class="row">
                <div class="col-md-12">
                    <img src="../img/banner.jpg" alt="Banner de Farmer's Market" width="1000px">
                </div>
            </div>
            <!-- Fin del Banner  -->
            <br>
            <!-- Contenedor Principal de la Página -->
            <div class="row">
                <!-- Dashboard -->
                <div class="col-md-2" style="background: #FAFAFA; border-radius: 3px">
                    <!-- Información del Rol iniciado -->
                    <div class="media">
                        <div class="media-left">
                            <a href="#" data-toggle="modal" data-target="#modalSubirFoto">
                                <img class="media-object img-circle" width="50" 
                                     <% if (actualUsuario.getImagen() != null) {

                                     %>
                                     src="<%=actualUsuario.getImagen()%>"
                                     <%
                                     } else if (actualUsuario.getImagen() == null) {

                                     %>
                                     src="../img/avatars/user.png"
                                     data-toggle="tooltip" data-original-title="Precione click para subir una foto"
                                     <%               }
                                     %>
                                     alt="Mi foto de perfil">
                            </a>
                        </div>
                        <div class="media-body">
                            <p></p>
                            <h4 class="media-heading">
                                Productor
                            </h4>
                            <%= actualUsuario.getNombres() + " " + actualUsuario.getApellidos()%>
                        </div>
                    </div>
                    <!-- Fin del rol iniciado -->
                    <hr>

                    <!-- Menú de navegación -->
                    <ul class="nav nav-pills nav-stacked" id="sesionPermisos">
                        <%
                            ArrayList<PermisoDto> listaPermisos;
                            listaPermisos = (ArrayList<PermisoDto>) faUsu.obtenerPermisosPorRol(primerRol.getIdRol());
                            for (PermisoDto temPermiso : listaPermisos) {
                        %>
                        <li role="presentation" class="
                            <% if (temPermiso.getUrl().equals(pagActual)) {
                                    out.print("active ");
                                }
                            %>
                            text-left">
                            <a href="<%= temPermiso.getUrl()%>"><%= temPermiso.getPermiso() + " " + temPermiso.getIcono()%></a>
                        </li>
                        <%
                            }
                        %>
                    </ul>
                    <!-- Fin del menú de navegación -->

                </div>
                <!-- Fin de la Dashboard -->


                <!-- Contenedor de Segundo-->
                <div class="col-md-10">
                    <!-- Menú de Sesion, buscar, idiomas y info -->
                    <nav class="navbar navbar-default">
                        <div class="container-fluid">

                            <div class="navbar-header">
                                <%
                                    for (RolDto rol : rolesActuales) {
                                        if (rol.getIdRol() == 1) {
                                %>
                                <a href="#" class="navbar-brand text-success">
                                    Pedidos <span class="badge info">4</span> 
                                </a>
                                <a href="#" class="navbar-brand text-success">
                                    Ofertas <span class="badge">18</span>
                                </a>
                                <%
                                } else if (rol.getIdRol() == 2) {
                                %>
                                <a href="#" class="navbar-brand text-success">
                                    Pedidos en espera <span class="badge info">4</span> 
                                </a>
                                <%
                                        }
                                    }
                                %>
                            </div>
                            <!-- Collect the nav links, forms, and other content for toggling -->
                            <div class="collapse navbar-collapse" id="bs-example-navbar-collapse-1">
                                <ul class="nav navbar-nav navbar-right">                                    
                                    <li><a href="#"><img src="../img/flag/ing/flag-ingles-16.png" alt="Cambiar idioma a Inglés" title="Cambiar idioma a Inglés"></a></li>
                                    <li><a href="#"><img src="../img/flag/spa/flag-spanis16.png" alt="Cambiar idioma a Español" title="Cambiar idioma a Español"></a></li>
                                    <li class="dropdown">
                                        <a href="#" class="dropdown-toggle" data-toggle="dropdown" role="button" aria-expanded="false"> <%= actualUsuario.getNombres() + " " + actualUsuario.getApellidos()%> <span class="fa fa-chevron-down"></span></a>
                                        <ul class="dropdown-menu" role="menu">
                                            <li class="text-center"><a href="../ControladorSesiones?op=salir">Cerrar Sesión</a></li>
                                            <li class="divider"></li>
                                            <li class="text-center"><a href="perfil.jsp">Mi Perfil</a></li>
                                            <li class="divider"></li>
                                            <li class="text-center"><a href="#" data-toggle="modal" data-target="#modalCambiarClave">Cambiar Contraseña</a></li>
                                            <li class="divider"></li>
                                            <li class="text-center"><a href="#">Ayuda <i class="fa fa-exclamation-circle"></i></a></li>
                                        </ul>
                                    </li>
                                </ul>
                                <form class="navbar-form navbar-right" role="search">
                                    <div class="form-group">
                                        <div class="input-group">                                            
                                            <input type="text" class="form-control" placeholder="¿Qué está buscando?...">
                                            <span class="input-group-btn">
                                                <button class="btn btn-default" type="submit">Buscar!</button>
                                            </span>
                                        </div>
                                    </div>
                                </form>

                                <ul class="navbar-form navbar-toggle">                                                                                
                                    <button class="btn btn-success navbar-brand" type="button">
                                        Pedidos <span class="badge">4</span>
                                    </button>
                                </ul>
                            </div>
                        </div>
                    </nav>
                    <!-- Fin de menú de sesión, buscar, idiomas y info -->

                    <!-- Miga de pan -->
                    <ol class="breadcrumb">
                        <li class="active"><a href="#">Inicio</a></li>
                    </ol>
                    <!-- Fin de miga de pan -->

                    <!-- Mensajes de alertas -->
                    <%
                        if (request.getParameter("msg") != null && request.getParameter("tipoAlert") != null) {
                    %>
                    <div class="alert alert-<%= request.getParameter("tipoAlert")%>" role="alert">
                        <button type="button" class="close" data-dismiss="alert" aria-label="Close">
                            <span aria-hidden="true">&times;</span>
                        </button>
                        <p class="text-center"><%= request.getParameter("msg")%></p>
                    </div>
                    <%
                        }
                    %>            
                    <!-- Fin de mensajes de alertas -->

                    <!-- Contenedor de contenido especifico -->
                    <div class="container-fluid">
                        <div class="row">
                            <div class="col-md-6">
                                <div class="page-header">
                                    <h3 class="text-center">Stock de productos</h3>
                                </div>
                                <table class="table table-bordered">
                                    <thead>
                                        <tr>
                                            <th>Producto</th>
                                            <th>Cantidad</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        <tr>
                                            <td>Cebolla</td>
                                            <td>
                                                <div class="progress">
                                                    <div class="progress-bar progress-bar-info progress-bar-striped active" role="progressbar" aria-valuenow="45" aria-valuemin="0" aria-valuemax="100" style="width: 91%">91%</div>
                                                </div>
                                            </td>
                                        </tr>
                                        <tr>
                                            <td>Papa</td>
                                            <td>
                                                <div class="progress">
                                                    <div class="progress-bar progress-bar-success progress-bar-striped active" role="progressbar" aria-valuenow="45" aria-valuemin="0" aria-valuemax="100" style="width: 45%">45%</div>
                                                </div>
                                            </td>
                                        </tr>
                                    </tbody>
                                </table>
                            </div>
                            <div class="col-md-6">
                                <div class="row">
                                    <!-- Imagen de productos destacados -->
                                    <center>
                                        <img src="../img/mas-vendidp.gif" alt="Prooductos destacados">
                                    </center>                                    
                                    <!-- Fin de imagen de productos destacados -->
                                    <br>
                                    <div class="col-md-6">                                        
                                        <div class="thumbnail">
                                            <img src="../img/descarga.svg" alt="...">
                                            <div class="caption">
                                                <h3>Thumbnail label</h3>
                                                <p>
                                                    Cras justo odio, dapibus ac 
                                                    facilisis in, egestas eget quam. 
                                                    Donec id elit non mi porta gravida at 
                                                    eget metus. Nullam id dolor id nibh 
                                                    ultricies vehicula ut id elit.
                                                </p>
                                                <p><a href="#" class="btn btn-primary" role="button">Button</a> <a href="#" class="btn btn-default" role="button">Button</a></p>
                                            </div>
                                        </div>
                                    </div>
                                    <div class="col-md-6">
                                        <div class="thumbnail">
                                            <img src="../img/descarga.svg" alt="...">
                                            <div class="caption">
                                                <h3>Thumbnail label</h3>
                                                <p>
                                                    Cras justo odio, dapibus ac 
                                                    facilisis in, egestas eget quam. 
                                                    Donec id elit non mi porta gravida at 
                                                    eget metus. Nullam id dolor id nibh 
                                                    ultricies vehicula ut id elit.
                                                </p>
                                                <p><a href="#" class="btn btn-primary" role="button">Button</a> <a href="#" class="btn btn-default" role="button">Button</a></p>
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>                        
                    </div>
                    <!-- Fin de contenedor de contenido especifico -->

                    <!-- Ventanas Modales -->
                    <div class="container-fluid">
                        <!-- Cambiar Contraseña -->
                        <div class="modal fade" id="modalCambiarClave" tabindex="-1" role="dialog" aria-labelledby="myModalLabel" aria-hidden="true">
                            <div class="modal-dialog">
                                <div class="modal-content">
                                    <div class="modal-header">
                                        <button type="button" class="close" data-dismiss="modal" aria-label="Close"><span aria-hidden="true">&times;</span></button>
                                        <h4 class="modal-title text-center" id="myModalLabel">Cambiar Contraseña</h4>
                                    </div>
                                    <div class="modal-body">

                                        <form class="form-horizontal" method="POST" action="../GestionUsuarios" id="formCambiarClave">
                                            <div class="form-group has-feedback" id="inpClaveAntigua">
                                                <label for="ccClaveAntigua" class="col-sm-4 control-label">Contraseña Antigua</label>
                                                <div class="col-sm-7">
                                                    <input type="password" class="form-control" 
                                                           id="ccClaveAntigua" placeholder="Ingrese la contraseña antigua"
                                                           name="ccClaveAntigua" onblur="validarClaveEnCambiar(this)">
                                                    <!-- Al momento de validar, se le manda la class a la i para agregar icon-->
                                                    <i id="iconFeedbackClaveCambiar"></i>
                                                </div>
                                            </div>

                                            <div class="form-group has-feedback" id="inpClaveNuevaCambiar">
                                                <label for="ccClaveNueva" class="col-sm-4 control-label">Nueva Contraseña</label>
                                                <div class="col-sm-7">                                                        
                                                    <input type="password" class="form-control" 
                                                           id="ccClaveNueva" placeholder="Ingrese una nueva contraseña"
                                                           name="ccClaveNueva" onblur="validarClaveNuevaEnCambiar(this)">
                                                    <!-- Al momento de validar, se le manda la class a la i para agregar icon-->
                                                    <i id="iconFeedbackClaveNuevaCambiar"></i>
                                                </div>
                                            </div>

                                            <div class="form-group has-feedback" id="inpClaveRepetidaCambiar">
                                                <label for="ccClaveRepetida" class="col-sm-4 control-label">Repetir Contraseña</label>
                                                <div class="col-sm-7">                                                        
                                                    <input type="password" class="form-control" 
                                                           id="ccClaveRepetida" placeholder="Ingrese una nueva contraseña"
                                                           name="ccClaveRepetida" onblur="validarRepetirClaveNuevaEnCambiar(this)">
                                                    <!-- Al momento de validar, se le manda la class a la i para agregar icon-->
                                                    <i id="iconFeedbackClaveNuevaCambiar2"></i>
                                                </div>
                                            </div>                                                

                                            <input hidden="true" name="ccViene" value="indexp">
                                            <input hidden="true" name="ccDocumento" id="ccDocumento" value="<%= actualUsuario.getIdUsuario()%>">
                                            <input hidden="true" name="formCambiarClave" id="formCambiarClave" value="ok">
                                        </form>                                                                                        
                                    </div>
                                    <div class="modal-footer">
                                        <button type="button" class="btn btn-danger" data-dismiss="modal">Cancelar</button>
                                        <button type="button" id="botonEnviarCambiarClave" class="btn btn-success"  onclick="enviarFormulario('formCambiarClave')">Cambiar Contraseña</button>
                                    </div>
                                </div>
                            </div>
                        </div>
                        <!-- Fin de Cambiar Contraseña -->

                        <!-- Formulario de Contáctenos -->                        
                        <div class="modal fade" id="modalContactenos" tabindex="-1" role="dialog" aria-labelledby="myModalLabel" aria-hidden="true">
                            <div class="modal-dialog">
                                <div class="modal-content">
                                    <div class="modal-header">
                                        <button type="button" class="close" data-dismiss="modal" aria-label="Close"><span aria-hidden="true">&times;</span></button>
                                        <h4 class="modal-title text-center" id="myModalLabel">Contáctenos | Farmer's Market</h4>
                                    </div>
                                    <div class="modal-body">
                                        <form class="form-horizontal" method="POST" action="../GestionUsuarios" id="formContactenos">
                                            <div class="form-group">
                                                <label for="mcNombre" class="col-sm-2 control-label">Nombre</label>
                                                <div class="col-sm-10">
                                                    <input type="text" class="form-control" name="mcNombre"
                                                           id="mcNombre" placeholder="Ingrese su nombre">
                                                </div>
                                            </div>

                                            <div class="form-group">
                                                <label for="mcCorreo" class="col-sm-2 control-label">Correo</label>
                                                <div class="col-sm-10">
                                                    <input type="text" class="form-control" name="mcCorreo"
                                                           id="mcCorreo" placeholder="Ingrese su correo electrónico">
                                                </div>
                                            </div>

                                            <div class="form-group">
                                                <label for="inputPassword3" class="col-sm-2 control-label">Mensaje</label>
                                                <div class="col-sm-10">
                                                    <textarea name="mcMensaje" class="form-control" rows="4" placeholder="Ingrese su mensaje para la compañía Farmer's Market"></textarea>
                                                </div>
                                            </div>

                                            <input hidden="true" name="mcViene" value="indexp">
                                            <input type="hidden" name="mcEnviar" value="ok">
                                        </form>
                                    </div>
                                    <div class="modal-footer">
                                        <button type="button" class="btn btn-danger" data-dismiss="modal">Cerrar</button>
                                        <button type="button" class="btn btn-success" onclick="enviarFormulario('formContactenos')">Enviar Mensaje</button>
                                    </div>
                                </div>
                            </div>
                        </div>
                        <!-- Fin de formulario de Contáctenos -->
                    </div>
                    <!-- Fin de Ventanas Modales-->

                </div>
                <!-- Contenedor de Segundo-->
            </div>
            <!-- Fin de contenedor Principal de la Página -->
            <p></p>
            <!-- Footer (Nota: Escribir el código que permita que esto quede abajo fijo) -->
            <div class="row">
                <div class="col-md-13">
                    <!-- Footer (Nota: Escribir el código que permita que esto quede abajo fijo) -->
                    <ol class="breadcrumb container-fluid">
                        <em class="text-center">Todos los derechos reservados / <a href="http://getbootstrap.com/">Bootstrap</a> / <a href="http://fortawesome.github.io/Font-Awesome/">Font-Awesome</a> / <a href="http://jquery.com/">JQuery</a></em>
                        <em class="pull-right"><a href="#" data-toggle="modal" data-target="#modalContactenos">Contactar un Administrador</a></em>
                    </ol>

                </div>
            </div>
            <!-- Fin del Footer -->
        </div>
    </body>
</html>
<%
        }
    }
%>
