<%-- 
    Document   : index
    Created on : 29/03/2015, 07:05:08 PM
    Author     : User
--%>

<%@page import="modulo.usuarios.FUsuario"%>
<%@page import="modulo.usuarios.dao.DepartamentoDao"%>
<%@page import="modulo.usuarios.dto.DepartamentoDto"%>
<%@page import="java.util.ArrayList"%>
<%@page contentType="text/html" pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <title>Inicio - FarmerMarket</title>
        <link rel="shortcut icon" href="img/favicon.ico">
        <script type="text/javascript" src="js/jquery-1.11.2.js"></script>
        <script type="text/javascript" src="js/Validacion.js"></script>
        <script type="text/javascript" src="js/bootstrap.js"></script>        
        <link rel="stylesheet" type="text/css" href="css/bootstrap.css">
        <link rel="stylesheet" type="text/css" href="css/font-awesome.css">        
        <script type="text/javascript" src="js/ajax.js"></script>
        <script type="text/javascript" src="js/validacionesAjax.js"></script>
    </head>
    <body>
        <%
            FUsuario faUsu = new FUsuario();

        %>
        <!--Inicio Contenedor del sitio -->
        <div class="container">
            <!--Inicio Banner Farmers Market-->
            <div class="row">
                <div class="col-md-12">
                    <a href="index.jsp"><img src="img/banner.jpg" alt="Banner Farmers Market"></a>
                </div>
            </div>
            <!--Fin Banner Farmers Market-->
            <!-- Mensajes de alertas -->
            <%                if (request.getParameter("msg") != null && request.getParameter("tipoAlert") != null) {
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
            <!--Inicio Contenedor Principal-->
            <div class="row">
                <!--Inicio Contendor Izquierdo Slide de imagenes-->
                <div class="col-md-8">
                    <!--Inicio slide de imagenes-->
                    <div id="carousel" class="carousel slide" data-ride="carousel">
                        <!--Inicio Indicador de imaganes-->
                        <ol class="carousel-indicators">
                            <li data-target="#carousel-" data-slide-to="0" class="active"></li>
                            <li data-target="#carousel" data-slide-to="1"></li>
                            <li data-target="#carousel" data-slide-to="2"></li>
                        </ol>
                        <!--Inicio Indicador de imaganes-->
                        <!--Inicio Imagenes-->
                        <div class="carousel-inner" role="listbox">
                            <div class="item active" >
                                <img src="img/carousel/wallhaven-102789.jpg" alt="...">
                                <!--Descripcion de la imagen-->
                                <div class="carousel-caption">
                                    Los mejores productos de colombia
                                </div>
                            </div>                            
                            <div class="item ">
                                <img src="img/carousel/wallhaven-107830.jpg" alt="...">
                                <!--Descripcion de la imagen-->
                                <div class="carousel-caption">
                                    Seleccionamos los mejores cultivos
                                </div>
                            </div>
                            <div class="item">
                                <img src="img/carousel/wallhaven-9277.jpg" alt="...">
                                <!--Descripcion de la imagen-->
                                <div class="carousel-caption">
                                    No podras obtenerlos en otro lugar
                                </div>
                            </div>
                            <!--Inicio Controles de desplazamiento-->
                            <a class="left carousel-control" href="#carousel" role="button" data-slide="prev">
                                <span class="glyphicon glyphicon-chevron-left" aria-hidden="true"></span>
                                <span class="sr-only">Anterior</span>
                            </a>
                            <a class="right carousel-control" href="#carousel" role="button" data-slide="next">
                                <span class="glyphicon glyphicon-chevron-right" aria-hidden="true"></span>
                                <span class="sr-only">Siguiente</span>
                            </a>
                            <!--Fin Controles de desplazamiento-->
                        </div>
                        <!--Fin Imagenes-->
                    </div>
                    <!--Fin slide de imagenes-->
                </div>
                <!--Fin Contenedor Izquierdo Slide de imagenes-->

                <!--Inicio Conetenedor Derecho-->
                <div class="col-md-4">
                    <div class="page-header">
                        <!--Inicio Bienvenida-->
                        <h1>Bienvenido(a) <br> 
                            <small>a Farmer's Market
                                <a href="#">
                                    <img src="img/flag/ing/flag-ingles-16.png" alt="Cambiar idioma a Inglés" title="Cambiar idioma a Inglés">
                                </a> 
                                <a href="#">
                                    <img src="img/flag/spa/flag-spanis16.png" alt="Cambiar idioma a Español" title="Cambiar idioma a Español">
                                </a>
                            </small>
                        </h1>
                    </div>
                    <!--Fin Bienvenida-->
                    <div>
                        <h3 class="text-center">El sitio perfecto para encontrar y vender tus productos</h3>
                        <p class="text-justify">
                            Aquí podrás encontrar los procesos de oferta y demanda 
                            que se ven en la cadena de comercialización agricola que se
                            vive en Colombia.
                        </p>
                        <p class="text-center text-success">
                            ¿Quieres hacerte parte de la comunidad?<br> ¿Qué estás esperando?
                        </p>
                        <br>
                        <p>
                            <a href="#" class="btn btn-success" role="button" data-toggle="modal" data-target="#modalRegistrarse">Registrárse en Farmer's Market</a>
                            <a href="#" class="btn btn-primary" role="button" data-toggle="modal" data-target="#modalIniciarSesion">Iniciar Sesión</a>                            
                        </p>
                    </div>
                </div>
                <!--Fin Contenedor Derecho--> 
            </div>
            <!--Fin Contenedor Principal-->
            <br>                
            <!--Informacion de contacto-->
            <div class="row">
                <div class="col-md-12">
                    <!-- Footer (Nota: Escribir el código que permita que esto quede abajo fijo) -->
                    <ol class="breadcrumb container-fluid">
                        <em class="text-center">Todos los derechos reservados / 
                            <a href="http://getbootstrap.com/">Bootstrap</a> / 
                            <a href="http://fortawesome.github.io/Font-Awesome/">Font-Awesome</a> / 
                            <a href="http://jquery.com/">JQuery</a>
                        </em>
                        <em class="pull-right">
                            <a href="#" data-toggle="modal" data-target="#modalContactenos">Contactar un Administrador</a>
                        </em>
                    </ol>
                </div>
            </div>
            <!--Fin Informacion de contacto-->
            <!--Inicio Contenedor Modal de Registro-->
            <div class="container">
                <!--Inicio Ventana Modal Registro-->
                <div class="modal fade" id="modalRegistrarse" tabindex="-1" role="dialog" aria-labelledby="myLargeModalLabel" aria-hidden="true">
                    <div class="modal-dialog modal-lg">                        
                        <div class="modal-content">
                            <!--Inicio Titulo de ventana modal-->
                            <div class="modal-header">
                                <button type="button" class="close" data-dismiss="modal" aria-label="Close">
                                    <span aria-hidden="true">&times;</span>
                                </button>
                                <h2 class="modal-title text-center text-success">Registrárse en Farmer's Market</h2>
                            </div>
                            <!--Fin Titulo de ventana modal-->
                            <br>
                            <!--Inicio Contendor del fomulario-->
                            <div class="container-fluid">
                                <div class="col-md-12">
                                    <form action="ControladorUsuarios" method="post" autocomplete="off" >
                                        <div class="col-md-6">
                                            <div class="form-group has-feedback" id="inpRol">
                                                <label for="ruRol" class="control-label">Yo soy:</label>
                                                <select name="ruRol" id="ruRol" class="form-control" tabindex="1" required autofocus onblur="validarRol(this)">
                                                    <option value="" selected="true">Seleccione un rol</option>
                                                    <option value="1">Productor</option>
                                                    <option value="2">Cliente</option>                                                        
                                                </select>
                                            </div>                                                                       

                                            <div class="form-group has-feedback" id="inpNombres">
                                                <label class="control-label" for="ruNombres">Nombres:</label>
                                                <input type="text" class="form-control" name="ruNombres" maxlength="25" onblur="validarNombres(this);" 
                                                       id="ruNombres" tabindex="2" value="" >
                                                <i id="iconFeedbackNombres"></i>
                                            </div>

                                            <div class="form-group has-feedback" id="inpApellidos">
                                                <label class="control-label" for="ruApellidos">Apellidos:</label>
                                                <input type="text" class="form-control" name="ruApellidos" maxlength="25"
                                                       id="ruApellidos" tabindex="3" value="" onblur="validarApellidos(this)">
                                                <i id="iconFeedbackApellidos"></i>
                                            </div>

                                            <div class="form-group has-feedback" id="inpDocumento">
                                                <label class="control-label" for="ruDocumento">N° de Documento:</label>
                                                <input type="text" class="form-control" tabindex="4" name="ruDocumento"
                                                       id="ruDocumento" value="" maxlength="10" onblur="validarDocumento(this)" >
                                                <i id="iconFeedbackDocumento"></i>
                                            </div>

                                            <div class="form-group has-feedback" id="inpClave">
                                                <label class="control-label" for="ruClave">Contraseña:</label>
                                                <input type="password" class="form-control" value="" name="ruClave" id="ruClave" tabindex="5" maxlength="45"  onblur="validarClave(this)" >
                                                <i id="iconFeedbackClave"></i>
                                            </div>                                           


                                            <div class="form-group has-feedback" id="inpDireccion">
                                                <label class="control-label" for="ruDireccion">Dirección:</label>
                                                <input type="text" class="form-control" name="ruDireccion" 
                                                       id="ruDireccion" tabindex="6" value="" maxlength="45" onblur="validarDireccion(this)" >
                                                <i id="iconFeedbackDireccion"></i>
                                            </div>
                                        </div>

                                        <div class="col-md-6">  

                                            <div class="form-group has-feedback" id="inpCorreo">
                                                <label class="control-label" for="ruCorreo">Correo:</label>
                                                <input type="text" class="form-control" name="ruCorreo" tabindex="7" onblur="validarCorreo(this);
                                                        validaCorreoYaRegistrado(this)"
                                                       id="ruCorreo" value="">
                                                <i id="iconFeedbackCorreo"></i>
                                            </div>

                                            <div class="form-group has-feedback" id="inpCorreoRepetido">
                                                <label class="control-label" for="ruCorreo2">Repetir Correo:</label>
                                                <input type="text" class="form-control" name="ruCorreo2" tabindex="8" onblur="validarRepetirCorreo(this)"
                                                       id="ruCorreo2" value="" >
                                                <i id="iconFeedbackCorreo2"></i>
                                            </div>

                                            <div class="form-group has-feedback" id="inpDepartamento">
                                                <label class="control-label" for="ruDepartamento">Departamento:</label>
                                                <select name="ruDepartamento" id="ruDepartamento" class="form-control" tabindex="9" onblur="validarDepartamento(this);" onchange="getSubcategorias(this.value);" >
                                                    <option value="">Seleccione un departamento</option>
                                                    <%
                                                        ArrayList<DepartamentoDto> listDepartamentos;
                                                        listDepartamentos = (ArrayList<DepartamentoDto>) faUsu.obtenerTodosDepartamentos();
                                                        for (DepartamentoDto d : listDepartamentos) {
                                                    %>
                                                    <option value="<%=d.getIdDepartamento()%>"><%=d.getDepartamento()%></option>                                                        
                                                    <%
                                                        }
                                                    %>
                                                </select>
                                            </div>

                                            <div class="form-group has-feedback" id="inpCiudad">
                                                <label class="control-label" for="ruCiudad">Ciudad:</label>
                                                <select name="ruCiudad" id="ruCiudad" class="form-control" tabindex="10" onblur="validarCiudad(this)" >
                                                    <option value="">Seleccione una ciudad</option>

                                                </select>
                                            </div>



                                            <div class="form-group has-feedback" id="inpFecha">
                                                <label class="control-label" for="ruFechaNacimiento">Fecha Nacimiento</label>
                                                <input type="date" class="form-control" name="ruFechaNacimiento" id="ruFecha" tabindex="11" onblur="validarFecha(this)">
                                                <i id="iconFeedbackFecha"></i>
                                            </div>

                                            <div class="checkbox has-feedback" id="inpTerminos">
                                                <label class="control-label">
                                                    <input  type="checkbox" id="ruTerminos" name="ruTerminos" tabindex="12" onblur="validarTerminos()"> Acepto <a href="#" data-toggle="modal" data-target="#modalTerminos">Terminos y Condiciones</a>
                                                </label>
                                            </div>

                                            <input type="submit" id="botonRegistro" name="botonRegistro" class="btn btn-success" value="Registrarme" >
                                        </div>
                                    </form>

                                </div>

                            </div>                
                            <!--Fin Contendor del fomulario-->
                            <br>
                        </div>

                    </div>

                </div>
                <!--Fin Ventana Modal Registro-->
            </div>
            <!--Fin Contenedor Modal de Registro-->
            <!--Inicio Ventana modal para Iniciar Sesión -->
            <div class="row">
                <div class="modal fade" id="modalIniciarSesion" tabindex="-1" role="dialog" aria-labelledby="mySmallModalLabel" aria-hidden="true">
                    <div class="modal-dialog modal-sm">
                        <div class="modal-content">
                            <!-- Título de la ventana de ingreso -->
                            <div class="modal-header">
                                <button type="button" onclick="limpiarIndex()" class="close" data-dismiss="modal" aria-label="Close"><span aria-hidden="true">&times;</span></button>
                                <h2 class="modal-title text-center text-info">Iniciar Sesión</h2>
                            </div>
                            <!-- Fin del título de la ventana de ingreso -->
                            <br>
                            <div class="container-fluid">
                                <form method="POST" action="ControladorSesiones" id="ingresarSistema">
                                    <div class="col-md-12">
                                        <div class="form-group" id="iuDocumento">
                                            <label class="control-label" for="isDocumento">N° Documento</label>
                                            <input type="text" class="form-control" id="isDocumento" value=""
                                                   name="isDocumento" maxlength="10" placeholder="Ingrese su documento" onblur="validarDocumentoIngreso(this)">
                                            <i id="inFeedbackDocumento"></i>
                                        </div>
                                        <div class="form-group" id="iuClave">
                                            <label class="control-label" for="isClave">Contraseña</label>
                                            <input type="password" class="form-control" id="isClave" value="" name="isClave" placeholder="Ingrese su contraseña" onblur="validarClaveIngreso(this)">
                                            <i id="inFeedbackClave"></i>
                                            <br>
                                            <em><a href="#" data-toggle="modal" data-target="#modalRecuperarClave" class="text-primary">¿Olvido su contraseña?</a></em>

                                        </div>
                                        <center>
                                            <input type="submit" name="botonIniciar" id="botonIniciar" class="btn btn-primary" value="Ingresar">
                                        </center>
                                        <br>
                                    </div>                                        
                                </form>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
            <!--Fin Ventana modal para Iniciar Sesión -->
            <!--Inicio Ventana modal para Términos y Condiciones -->
            <div class="row">
                <!-- Modal -->
                <div class="modal fade" id="modalTerminos" tabindex="-1" role="dialog" aria-labelledby="myModalLabel" aria-hidden="true">
                    <div class="modal-dialog">
                        <div class="modal-content">
                            <div class="modal-header">
                                <button type="button" class="close" data-dismiss="modal" aria-label="Close"><span aria-hidden="true">&times;</span></button>
                                <h4 class="modal-title" id="myModalLabel">Términos y Condiciones</h4>
                            </div>
                            <div class="modal-body">
                                <p>Aquí deben de ir los términos y condiciones</p>
                            </div>
                            <div class="modal-footer">
                                <button type="button" class="btn btn-default" data-dismiss="modal">Cerrar</button>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
            <!-- Fin de ventana modal para Términos y Condiciones -->
            <!-- Ventana Modal para recuperar Contraseña -->
            <div class="row">
                <div class="modal fade" id="modalRecuperarClave" tabindex="-1" role="dialog" aria-labelledby="myModalLabel" aria-hidden="true">
                    <div class="modal-dialog">
                        <div class="modal-content">
                            <div class="modal-header">
                                <button type="button" class="close" data-dismiss="modal" aria-label="Close"><span aria-hidden="true">&times;</span></button>
                                <h4 class="modal-title text-center text-success" id="myModalLabel">Recuperar contraseña</h4>
                            </div>
                            <div class="container-fluid">
                                <form method="POST" action="#" id="formRecuperarClave" >
                                    <div class="col-md-12">
                                        <div class="form-group" id="eCorreo">                                            
                                            <div class="form-group">
                                                <label for="recipient-name" class="control-label">Correo Electrónico:</label>
                                                <input type="text" class="form-control" id="rcCorreo" name="rcCorreo" value=""
                                                       placeholder="Ingrese su correo electrónico" onblur="validarOlvidada(this)">
                                            </div>                                            
                                        </div>                            
                                        <input type="hidden" class="form-control" id="rcEnviar" name="rcEnviar" value="ok">
                                        <br>
                                    </div>                                        
                                </form>
                            </div>
                            <div class="modal-footer">
                                <button type="button" class="btn btn-success" id="recuperar" onclick="enviarFormulario('formRecuperarClave')">Recuperar Contraseña</button>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
            <!-- Fin de ventana Modal para recuperar Contraseña -->
            <!-- Formulario de Contáctenos -->                        
            <div class="modal fade" id="modalContactenos" tabindex="-1" role="dialog" aria-labelledby="myModalLabel" aria-hidden="true">
                <div class="modal-dialog">
                    <div class="modal-content">
                        <div class="modal-header">
                            <button type="button" class="close" data-dismiss="modal" aria-label="Close"><span aria-hidden="true">&times;</span></button>
                            <h4 class="modal-title text-center text-success" id="myModalLabel">Contáctenos | Farmer's Market</h4>
                        </div>
                        <div class="modal-body">
                            <form class="form-horizontal" method="POST" action="GestionUsuarios" id="formContactenos">
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
                                <div class="text-right">
                                    <input type="submit" name="mcEnviar" class="btn btn-success" value="Enviar Mensaje">
                                </div>
                            </form>
                        </div>                       
                    </div>
                </div>
            </div>
            <!-- Fin de formulario de Contáctenos -->
        </div>
        <!--Fin Contenedor del sitio -->
    </body>
</html>
