<%-- 
    Document   : cargarPublicarOferta
    Created on : 24/03/2015, 12:05:25 AM
    Author     : kennross
--%>

<%@page import="modulo.usuarios.FUsuario"%>
<%@page import="modulo.ofertas.FOferta"%>
<%@page import="modulo.usuarios.dto.UsuarioDto"%>
<%@page import="modulo.ofertas.dto.ProductoDto"%>
<%@page import="modulo.ofertas.dto.OfertaDto"%>
<%@page contentType="text/html" pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <link rel="stylesheet" href="//code.jquery.com/ui/1.11.4/themes/smoothness/jquery-ui.css">
        <script src="//code.jquery.com/jquery-1.10.2.js"></script>
        <script src="//code.jquery.com/ui/1.11.4/jquery-ui.js"></script>        
        <title>Oferta</title>
    </head>
    <body>
        <%
            OfertaDto oDto;
            ProductoDto pDto;

            FOferta faOfer = new FOferta();

            int idProductoAso = Integer.parseInt(request.getParameter("idProducoAso"));

            pDto = faOfer.obtenerProductoConIdProductoAso(idProductoAso);
        %>       


        <strong><%= pDto.getNombres()%></strong> (<%= faOfer.obtenerNombreDeCategoriaPorId(pDto.getIdCategoria())%>)
        <input type="hidden" name="idProductoAsociado" value="<%=idProductoAso%>">
    </body>
</html>
