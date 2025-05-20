Simplification of the dataframe to its basic form
=================================================

Afin de ramener un dataframe à sa plus simple expression, c'est-à-dire un dataframe contenant seulement les colonnes de base, l'information des colonnes ajoutées devra être ramenée dans la colonne de base pour ne pas perdre cette information.
			
L'ordre de réduction des différentes colonnes est important et doit être respecté, particulièrement en ce qui concerne les ips, intervalles et forecast_hour.
			
Pour simplifier la compréhension, appelons les colonnes de base les colonnes maîtres, et les colonnes ajoutées les sous-colonnes.
			
Lors de la réduction de sous-colonnes, il est à noter que lorsque certaines sous-colonnes du groupe sont absentes (elles peuvent avoir été supprimées par l'usager), on doit d'abord recréer ces sous-colonnes à partir de l'information de la colonne maître, avant de pourvoir procéder à la mise-à-jour de la colonne maître.  Évidemment, si toutes les sous-colonnes d'un groupe sont absentes, il n'y a aucun travail à faire.
		
Pour ce qui est des colonnes ayant rapport à l'etiket et aux flags de modification, l'ordre importe peu.
			
#. Réduction des colonnes reliées à *etiket*
    * Sous-colonnes:  label, run, implementation, ensemble_member et etiket_format
    * La sous-colonne etiket_format est conservée pour des besoins de formatage de l'étiquette
    * La colonne maitre etiket est remise-à-jour avec les informations des colonnes mentionnées plus-haut.

#. Réduction des colonnes reliées aux flags de modification (*typvar*) 
    * Sous-colonnes:  masked, masks, missing_data, ensemble_extra_info, multiple_modifications, zapped, bounded, filtered, interpolated, unit_converted 
    * La colonne typvar est remise-a-jour avec les informations des sous-colonnes mentionnées de la façon suivante: 
        * Le 2ème caractère du typvar contient l'information correspondant aux flags :  
          H (missing_data), ! (ensemble_extra_info), M (multiple_modifications), Z(zapped), 
          B(bounded), F(filtered), I(interpolated), U (unit_converted) 
        * Lorsque plusieurs flags sont à TRUE, le 2ème caractère contiendra **M** for multiple_modifications à l'exception 
          des cas mentionnés ci-dessous
        * Lorsque masked et ensemble_extra_info sont à TRUE, typvar sera égal à !@
        * Lorsque le flags masks est à TRUE, typvar sera égal à @@

**Réduction des autres colonnes :**

#. Réduction de la colonne *forecast_hour* reliée aux colonnes *deet* et *npas*
    * Sous-colonne: forecast_hour
    * La colonne est mise-à-jour de la façon suivante :      
      npas = forecast_hour / deet
    * Si deet = 0, aucune mise-à-jour n'est faite au npas.

#. Réduction des colonnes *date_of_observation* et *date_of_validity* reliées à *dateo* et *datev* 
    * Sous-colonnes:  date_of_observation et date_of_validity
    * La colonne dateo est mise-à-jour avec l'info de date_of_observation
    * La colonne datev est mise-à-jour de la façon suivante : 
      datev = dateo + deet * npas

#. Réduction des colonnes reliées aux ips et intervalles
    * Sous-colonnes:  level, ip1_kind, ip1_kind, ip2_dec, ip2_kind, ip2_pkind, ip3_dec, ip3_kind, ip3_pkind, interval, surface, follow_topography, ascending
    * Les sous-colonnes ip1_pkind, ip2_pkind et ip3_pkind ainsi que surface, follow_topography et ascending sont ignorées   
    * Traitement pour les ips:
        #. Mise-à-jour de la colonne ip1 à partir de level et ip1_kind
        #. Mise-à-jour de la colonne ip2 à partir de ip2_dec et ip2_kind
        #. Si sous-colonne forecast_hour est présente : mise-à-jour de la colonne ip2 à partir de forecast_hour (préséance sur l'étape précédente)
        #. Mise-à-jour de la colonne ip3 à partir de ip3_dec et ip3_kind
        #. Mise-à-jour des colonnes ip1, ip2 et ip3 à partir de la colonne interval car cette information a préséance sur toutes les autres étapes



Il est à noter que même si la colonne *unit* ne fait pas partie des colonnes de base, lorsqu'on réduit le dataframe à sa plus 
simple expression, on conserve cette colonne lorsqu'elle est présente.  
Si elle est conservée, on doit alors s'assurer que toutes les valeurs sont remplies pour chacune des rangées du dataframe. 
