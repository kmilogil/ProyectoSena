/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package controladores.usuarios;

import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import modulo.usuarios.FUsuario;
import modulo.usuarios.dto.RolDto;
import modulo.usuarios.dto.UsuarioDto;

/**
 *
 * @author Akashyro
 */
public class ControladorUsuarios extends HttpServlet {

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
        FUsuario faUsu = new FUsuario();
        String salida;
        if (request.getParameter("botonRegistro") != null) {
            RolDto suRol = new RolDto();
            suRol.setIdRol(Integer.parseInt(request.getParameter("ruRol")));

            UsuarioDto nuevoUsuario = new UsuarioDto();
            nuevoUsuario.setIdUsuario(Long.parseLong(request.getParameter("ruDocumento")));
            nuevoUsuario.setNombres(request.getParameter("ruNombres"));
            nuevoUsuario.setApellidos(request.getParameter("ruApellidos"));
            nuevoUsuario.setClave(request.getParameter("ruClave"));
            nuevoUsuario.setCorreo(request.getParameter("ruCorreo"));
            nuevoUsuario.setFechaNacimiento(request.getParameter("ruFechaNacimiento"));
            nuevoUsuario.setDireccion(request.getParameter("ruDireccion"));
            nuevoUsuario.setIdCiudad(Integer.parseInt(request.getParameter("ruCiudad")));
            nuevoUsuario.setImagen(null);
            nuevoUsuario.setEstado(1);
            
            salida = faUsu.registrarUsuario(nuevoUsuario, suRol);            
            

            if (salida.equals("ok")) {
                response.sendRedirect("index.jsp?msg=<strong><i class='glyphicon glyphicon-ok'></i> ¡Registro Éxitoso!</strong> Revise su correo para activar cuenta, puede iniciar sesión.&tipoAlert=success");
            } else if (salida.equals("okno")) {
                response.sendRedirect("index.jsp?msg=<strong><i class='glyphicon glyphicon-exclamation-sign'></i> ¡Algo salió mal!</strong> Por favor intentelo de nuevo.&tipoAlert=warning");
            } else {
                response.sendRedirect("index.jsp?msg=<strong><i class='glyphicon glyphicon-exclamation-sign'></i> ¡Ocurrió un error!</strong> Detalle: " + salida + "&tipoAlert=danger");
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
