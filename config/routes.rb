KpiAdmin::Engine.routes.draw do
  root to: 'kpis#index'
  resources :kpis, only: :index
  %i(pv_uu search_num sign_in table rr).each do |name|
    get name, to: "kpis##{name}", as: name
  end
end
