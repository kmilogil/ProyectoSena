<%-- 
    Document   : plantilla
    Created on : 27/02/2015, 12:22:03 PM
    Author     : Akashyro
--%>

<%@page import="modulo.ofertas.dto.OfertaDto"%>
<%@page import="modulo.ofertas.FOferta"%>
<%@page import="modulo.usuarios.FUsuario"%>
<%@page import="modulo.usuarios.dto.PermisoDto"%>
<%@page import="modulo.usuarios.dao.PermisoDao"%>
<%@page import="modulo.usuarios.dto.RolDto"%>
<%@page import="java.util.ArrayList"%>
<%@page import="modulo.usuarios.dto.UsuarioDto"%>
<%@page contentType="text/html" pageEncoding="UTF-8"%>

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
        FOferta faOfer = new FOferta();
        String pagActual = "ofertas.jsp";

        // Validación para poder entrar
        boolean poderEntrar = false;

        for (RolDto rol : rolesActuales) {
            if (rol.getIdRol() == 2) {
                poderEntrar = true;
            }
        }

        if (poderEntrar) {

%>

<!DOCTYPE html>
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <link rel="shortcut icon" href="../img/favicon.ico">
        <link rel="stylesheet" type="text/css" href="../css/bootstrap.css">
        <link rel="stylesheet" type="text/css" href="../css/font-awesome.css">
        <script type="text/javascript" src="../js/jquery-1.11.2.js"></script>
        <script type="text/javascript" src="../js/bootstrap.js"></script>
        <script type="text/javascript" src="../js/ajax.js"></script>
        <script type="text/javascript" src="../js/Validacion.js"></script>
        <script type="text/javascript" src="../js/validacionesAjax.js"></script>
        <script type="text/javascript" src="../js/jquery.dataTables.min.js"></script>
        <script type="text/javascript" src="../js/dataTables.bootstrap.js"></script>
        <title>Ofertas - Farmer's Market</title>
        <script type="text/javascript">
            $(document).ready(function () {
                // Initialize tooltip
                $('[data-toggle="tooltip"]').tooltip({
                    placement: 'top'
                });
            });
        </script>
        <script>
            $(document).ready(function () {
                $("#cantidadPedir").keydown(function (event) {
                    if (event.shiftKey)
                    {
                        event.preventDefault();
                    }

                    if (event.keyCode === 46 || event.keyCode === 8) {
                    }
                    else {
                        if (event.keyCode < 95) {
                            if (event.keyCode < 48 || event.keyCode > 57) {
                                event.preventDefault();
                            }
                        }
                        else {
                            if (event.keyCode < 96 || event.keyCode > 105) {
                                event.preventDefault();
                            }
                        }
                    }
                });
            });
        </script>
        <script>
            $(document).ready(function () {
                $('#paOfertas').dataTable();
            });

        </script>
    </head>
    <body >
        <div class="container">
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
                                     data-toggle="tooltip" title="Precione click para subir una foto"
                                     <%               }
                                     %>
                                     alt="Mi foto de perfil">
                            </a>
                        </div>
                        <div class="media-body">
                            <p></p>
                            <h4 class="media-heading">
                                Cliente
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
                                    out.print("active");
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
                                <a href="#" class="navbar-brand text-success">
                                    Pedidos en espera <span class="badge info">4</span> 
                                </a>                                
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
                        <li><a href="indexp.jsp">Inicio</a></li>
                        <li class="active"><a href="ofertas.jsp">Ofertas</a></li>                    
                    </ol>
                    <!-- Fin de miga de pan -->

                    <!-- Mensajes de alertas -->
                    <%                        if (request.getParameter("msg") != null && request.getParameter("tipoAlert") != null) {
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
                            <div class="col-md-9">

                                <!-- Titutlo -->
                                <div class="page-header">
                                    <h2 class="text-center">Ofertas</h2>
                                </div>
                               
                                
                                <%
                                    ArrayList<OfertaDto> ofertas = new ArrayList();
                                    ofertas = (ArrayList<OfertaDto>) faOfer.obtenerOfertas(actualUsuario.getIdUsuario(),"a",0,0);
                                    if (ofertas.size() != 0) {
                                        for (OfertaDto oferta : ofertas) {

                                %>

                                <div class="panel panel-success" >
                                    <div class="panel-heading">
                                        <h2 class="panel-title">
                                            Por <em><strong><a href="#"><%=oferta.getProAso().getUsDto().getNombres()%></a></strong></em>                                            
                                        </h2>
                                    </div>
                                    <div class="panel-body" >
                                        <table class="table">
                                            <thead>
                                                <tr>
                                                    <th>Producto</th>
                                                    <th>Cantidad</th>
                                                    <th>Precio por producto</th>
                                                    <th>Presentación</th>
                                                </tr>
                                            </thead>

                                            <tr>                                                    
                                                <td><%=oferta.getProAso().getProDto().getNombres()%></td>
                                                <td><%=oferta.getInDto().getCantidad()%></td>
                                                <th><%=oferta.getPrecioVenta()%></th>
                                                <td><%=oferta.getPreDto().getDescripcion()%></td>                                                    
                                            </tr>                                                             
                                        </table>
                                        <span class="lead">Fecha de vencimiento:</span> <%=oferta.getFechaFin()%>

                                        <% OfertaDto ofer = faOfer.obtenerOfertaPorId(oferta.getIdOferta());%>
                                        <span class="pull-right"><span class="lead">Promoción: </span><%=ofer.getProDto().getDescripcion()%></span>
                                    </div>
                                    <div class="panel-footer">
                                        <h3 class="panel-title">
                                            <!-- Novedades -->
                                            <!-- link para modal para agregar novedad -->                                            
                                            <a href="" data-toggle="modal" data-target="#modalRealizarPedido"  id="loadPedido" onclick="getPedido(<%=oferta.getIdOferta()%>);
                                                    tiempoCantidad();">
                                                <i class="fa fa-cart-arrow-down pull-left"></i> <span class="pull-left text-success" >Anadir al carrito</span>
                                            </a>
                                            <!-- link para modal para mostrar novedades 
                                            <a href="#" data-toggle="modal" data-target="#modalPromociones" onclick="getOfertaNovedad(<%/*=oferta.getIdOferta()*/%>)">
                                                <i class="fa fa-plus pull-right"> </i> <span class="pull-right text-success">Ver Promocion(es)</span>
                                            </a>                                                                                        
                                            <!-- Fin de novedades -->
                                            <br>
                                        </h3>
                                    </div>
                                </div>
                                <%                                        }
                                } else {
                                %>
                                <div class="col-md-12">
                                    <div class="alert alert-info text-center">
                                        <button type="button" class="close" data-dismiss="modal" aria-label="Close"><span aria-hidden="true">&times;</span></button>
                                        <p>
                                            <strong>No hay se han encontrado ofertas</strong> Revize mas tarde

                                        </p>
                                    </div>
                                </div>
                                <%
                                    }
                                %>  
                            </div>                                                                     


                            <!-- Fin de formato de un pedido -->
                            <!-- Publicidad-->
                            <div class="col-md-3">
                                <img src="../img/bann.jpg" alt="Publicidad de frutas">                                
                            </div>
                            <!-- Fin de publicidad -->
                        </div>                        
                    </div>                        
                </div>
                <!-- Fin de contenedor de contenido especifico -->


                <!-- Ventanas Modales -->
                <div class="container-fluid">
                    <!-- Cambiar Contraseña -->
                    <div>
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
                    </div>
                    <!-- Fin de Cambiar Contraseña -->                        

                    <!-- Formulario de Contáctenos -->
                    <div>
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

                                            <input hidden="true" name="mcViene" value="misPedidos">
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
                    </div>

                    <!-- Fin de formulario de Contáctenos -->

                    <!-- Listado de novedades por pedido -->
                    <div class="modal fade" id="modalPromociones">
                        <div class="modal-dialog">
                            <div class="modal-content">
                                <div class="modal-header">
                                    <button type="button" class="close" data-dismiss="modal" aria-label="Close"><span aria-hidden="true">&times;</span></button>
                                    <h3 class="modal-title">Promociones de la oferta</h3>
                                </div>
                                <div class="modal-body">
                                    <h4 class="text-center" id="formularioPromociones"></h4>
                                </div>
                                <div class="modal-footer">
                                    <button type="button" class="btn btn-danger" data-dismiss="modal">Cerrar</button>
                                </div>
                            </div>
                        </div>
                    </div>
                    <!-- Fin de listado de novedades por pedido -->

                    <!--modal Realizar pedido -->
                    <div class="modal fade" id="modalRealizarPedido" tabindex="-1" role="dialog" aria-labelledby="myModalLabel" aria-hidden="true">
                        <div class="modal-dialog">
                            <div class="modal-content">
                                <div class="modal-header">
                                    <button type="button" onclick="limpiar();
                                            quitarIntervalo()" class="close" data-dismiss="modal" aria-label="Close"><span aria-hidden="true">&times;</span></button>
                                    <h3 class="modal-title text-center" id="myModalLabel">Realizar Pedido</h3>
                                </div>
                                <div class="container-fluid" >
                                    <form  id="fomularioRealizarPedido" action="../ControladorPedido" method="post" autocomplete="off">
                                        <legend class="text-center" id="formularioRealizarPedido"></legend>

                                        <div class="form-group" id="pdCantidad">
                                            <label for="cantidadPedir" class="col-sm-3 control-label">Cantidad a pedir</label>
                                            <div class="col-sm-9">
                                                <div class="input-group">
                                                    <span class="input-group-addon">#</span>
                                                    <input type="text" class="form-control solo-numero" name="cantidadPedir" id="cantidadPedir" value="" maxlength="9" onclick="tiempoCalculo();" onblur="validarCantidadPedido(this)"> 

                                                </div>
                                            </div>
                                        </div>
                                        <br><br><br>
                                        <div class="form-group">
                                            <label for="Total" class="col-sm-3 control-label">Total :</label>
                                            <div class="col-sm-9">
                                                <div class="input-group">
                                                    <span class="input-group-addon">$</span>
                                                    <p class="form-control" readonly="true" name="Total" id="total"></p>
                                                    <span class="input-group-addon">Pesos</span>
                                                </div>
                                            </div>
                                        </div> 
                                        <br><br>
                                        <input type="hidden" name="usuario" value="<%=actualUsuario.getIdUsuario()%>">                                        
                                        <div class="modal-footer">
                                            <button type="button" class="btn btn-danger" data-dismiss="modal" onclick="limpiar();
                                                    quitarIntervalo()">Cancelar</button>
                                            <input type="submit" class="btn btn-success" id="pedirProducto" name="pedirProducto" value="Anadir al carrito">
                                        </div>
                                    </form>
                                </div>

                            </div>
                        </div>
                    </div>
                    <!--Fin modal realizar pedido-->
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