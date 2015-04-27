/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package controladores.ofertas;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import modulo.ofertas.FOferta;
import modulo.ofertas.dto.OfertaDto;
import modulo.usuarios.FUsuario;
import utilidades.Correo;

/**
 *
 * @author User
 */
public class ControladorOferta extends HttpServlet {

    /**
     * Processes requests for both HTTP <code>GET</code> and <code>POST</code>
     * methods.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    protected void processRequest(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        response.setContentType("text/html;charset=UTF-8");
        FOferta faOfer = new FOferta();
        FUsuario faUsu = new FUsuario();
        String salida;
        String correo;
        String mensajeCorreo;
        //Asociar un Producto
        if (request.getParameter("apEnviar") != null && request.getParameter("apEnviar").equals("Asociar")) {
            String codeProductosEnString[];
            long idProductor = Long.parseLong(request.getParameter("apidProductor"));
            codeProductosEnString = request.getParameterValues("apProductos");
            
            if (codeProductosEnString == null) {
                response.sendRedirect("pages/asociarproducto.jsp?msg=<strong><i class='glyphicon glyphicon-exclamation-sign'></i> ¡Debe seleccionar al menos un producto!</strong> &tipoAlert=warning");
            } else {
                int[] codeProductosInt = new int[codeProductosEnString.length];
                
                for (int i = 0; i < codeProductosEnString.length; i++) {
                    codeProductosInt[i] = Integer.parseInt(codeProductosEnString[i]);
                }                
                
                salida = faOfer.asociarProductos(idProductor, codeProductosInt);

//                mensajeCorreo = "Se le han asociado unos productos a petición suya.";
                if (salida.equals("ok")) {//                    
                    //Respuesta afirmativa y no se envío de correo
                    response.sendRedirect("pages/asociarproducto.jsp?msg=<strong>¡Asociasión Éxitosa! <i class='glyphicon glyphicon-ok'></i></strong> Mírelos en Mis Productos.&tipoAlert=success");
                    
                } else if (salida.equals("okno")) {
                    //Respuesta desconocida, no se realizó
                    response.sendRedirect("pages/asociarproducto.jsp?msg=<strong><i class='glyphicon glyphicon-exclamation-sign'></i> ¡Algo salió mal!</strong> Por favor intentelo de nuevo.&tipoAlert=warning");
                } else {
                    //Respuesta conocida, no se realizó
                    response.sendRedirect("pages/asociarproducto.jsp?msg=<strong><i class='glyphicon glyphicon-exclamation-sign'></i> ¡Ocurrió un error!</strong> Detalle: " + salida + "&tipoAlert=danger");
                }
                
            }

            //Eliminar Producto Asociado
        } else if (request.getParameter("op") != null && request.getParameter("op").equals("eliaso")) {
            int idProductoAso = Integer.parseInt(request.getParameter("idProductoAso"));
            int ofertado = faOfer.obtenerProductoOfertado(idProductoAso);
            if (ofertado == 0) {
                
                salida = faOfer.eliminarAsociasionDeProducto(idProductoAso);
                
                if (salida.equals("ok")) {
                    //Respuesta afirmativa y envío de corre
                    response.sendRedirect("pages/misproductos.jsp?msg=<strong>¡Eliminación Éxitosa! <i class='glyphicon glyphicon-ok'></i></strong> Se desasocio el producto.&tipoAlert=success");
                } else if (salida.equals("okno")) {
                    //Respuesta desconocida, no se realizó
                    response.sendRedirect("pages/misproductos.jsp?msg=<strong><i class='glyphicon glyphicon-exclamation-sign'></i> ¡Algo salió mal!</strong> Por favor intentelo de nuevo.&tipoAlert=warning");
                } else {
                    //Respuesta conocida, no se realizó
                    response.sendRedirect("pages/misproductos.jsp?msg=<strong><i class='glyphicon glyphicon-exclamation-sign'></i> ¡Ocurrió un error!</strong> Detalle: " + salida + "&tipoAlert=danger");
                }
            } else {
                response.sendRedirect("pages/misproductos.jsp?msg=<strong><i class='glyphicon glyphicon-exclamation-sign'></i> ¡El producto se encuentra en oferta!</strong> No puede eliminar un producto, mientras se encuentre en oferta.&tipoAlert=warning");
            }
        } else if (request.getParameter("productoOferta") != null) {            
            OfertaDto ofDto = new OfertaDto();
            ofDto.setIdProdAsoc(Integer.parseInt(request.getParameter("idProductoAsociado")));
            ofDto.setPrecioVenta(Long.parseLong(request.getParameter("PrecioVenta")));
            ofDto.setIdPromocion(Integer.parseInt(request.getParameter("promocion")));
            ofDto.setIdPresentacion(Integer.parseInt(request.getParameter("presentacion")));
            ofDto.getInDto().setCantidad(Integer.parseInt(request.getParameter("cantidad")));
            int out = faOfer.ofertarProducto(ofDto);
            if (out == 1) {
                response.sendRedirect("pages/misproductos.jsp?msg=<strong>¡Su oferta a sido publicada! <i class='glyphicon glyphicon-ok'></i></strong> Puede mirarla en Mis Ofertas &tipoAlert=success");
            } else {
                response.sendRedirect("pages/misproductos.jsp?msg=<strong><i class='glyphicon glyphicon-exclamation-sign'></i> ¡Algo salió mal!</strong> Por favor intentelo de nuevo.&tipoAlert=warning");
            }
        } else if (request.getParameter("abastecerOferta") != null) {
            
            salida = faOfer.abastecerInventario(Integer.parseInt(request.getParameter("aCantidad")), Integer.parseInt(request.getParameter("idOfert")));
            if (salida.equals("ok")) {
                response.sendRedirect("pages/misofertas.jsp?msg=<strong>¡Su inventario a sido abastecido! <i class='glyphicon glyphicon-ok'></i></strong> &tipoAlert=success");
            } else if (salida.equals("no")) {
                response.sendRedirect("pages/misofertas.jsp?msg=<strong><i class='glyphicon glyphicon-exclamation-sign'></i> ¡No se logro abastecer su inventario!</strong>&tipoAlert=warning");
            }
        }
    }

    // <editor-fold defaultstate="collapsed" desc="HttpServlet methods. Click on the + sign on the left to edit the code.">
    /**
     * Handles the HTTP <code>GET</code> method.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        processRequest(request, response);
    }

    /**
     * Handles the HTTP <code>POST</code> method.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        processRequest(request, response);
    }

    /**
     * Returns a short description of the servlet.
     *
     * @return a String containing servlet description
     */
    @Override
    public String getServletInfo() {
        return "Short description";
    }// </editor-fold>

}
